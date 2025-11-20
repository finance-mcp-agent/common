import os, json, time, threading
from typing import Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import boto3

# export KAFKA_BOOTSTRAP="b-1.trader-msk.xxxxxx.kafka.us-east-1.amazonaws.com:9096,b-2..."
# Local dev notes:
#   - Set KAFKA_MODE=local and (optionally) KAFKA_BOOTSTRAP=localhost:9092 to use a local Kafka broker.
#   - For MSK (default), omit KAFKA_MODE or set to "msk"; credentials are pulled from AWS Secrets Manager.
#   - You can also force protocol via KAFKA_SECURITY=[PLAINTEXT|SASL_SSL] (defaults to PLAINTEXT for local, SASL_SSL for MSK).

class KafkaBase:
    def __init__(self):
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.bootstrap = os.getenv("KAFKA_BOOTSTRAP", "")
        # Mode detection: explicit KAFKA_MODE wins; otherwise infer from bootstrap
        self.mode = os.getenv("KAFKA_MODE", "local").lower()
        if not self.mode:
            if self.bootstrap and ("localhost" in self.bootstrap or "127.0.0.1" in self.bootstrap):
                self.mode = "local"
            else:
                self.mode = "msk"
        if not self.bootstrap:
            # Only look up MSK bootstrap from SSM when in MSK mode
            self.bootstrap = self._lookup_bootstrap() if self.mode == "msk" else "localhost:9092"
        # Load auth only for MSK with SASL
        if self.mode == "msk":
            self.username, self.password = self._load_scram()
        else:
            self.username, self.password = "", ""

    def _lookup_bootstrap(self) -> str:
        ssm = boto3.client("ssm", region_name=self.region)
        return ssm.get_parameter(Name="/trader/dev/kafka_bootstrap")["Parameter"]["Value"]

    def _is_local(self) -> bool:
        if self.mode == "local":
            return True
        # Heuristic fallback
        return any(h in (self.bootstrap or "") for h in ["localhost", "127.0.0.1"])

    def _load_scram(self):
        if self._is_local():
            # No credentials needed for PLAINTEXT local brokers
            return "", ""
        sm = boto3.client("secretsmanager", region_name=self.region)
        sec = sm.get_secret_value(SecretId="AmazonMSK_trader_dev_kafka_scram")["SecretString"]
        creds = json.loads(sec)
        return creds["username"], creds["password"]

    def _config(self):
        # Allow override; if unset choose sensible defaults by mode.
        security = os.getenv("KAFKA_SECURITY", "").upper()
        if not security:
            security = "PLAINTEXT" if self._is_local() else "SASL_SSL"

        base = {"bootstrap.servers": self.bootstrap}

        if security == "PLAINTEXT":
            # Local dev: no SASL, no TLS
            return base
        elif security == "SASL_SSL":
            # MSK with SCRAM-SHA-512
            return {
                **base,
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "SCRAM-SHA-512",
                "sasl.username": self.username,
                "sasl.password": self.password,
            }
        else:
            # Fallback to PLAINTEXT if an unknown value is provided
            return base

    def ensure_topic(self, topic: str, partitions: int = 3, replication_factor: int = 2):
        """
        Create topic if missing. Works for both local (PLAINTEXT) and MSK (SASL_SSL) using the same client config.
        """
        admin = AdminClient(self._config())
        md = admin.list_topics(timeout=10)
        if topic in md.topics:
            return
        fs = admin.create_topics([NewTopic(topic, partitions, replication_factor)])
        try:
            fs[topic].result(30)
            print(f"[Kafka] Created topic: {topic}")
        except Exception as e:
            print(f"[Kafka] Topic {topic} may already exist: {e}")

class KafkaProducerClient(KafkaBase):
    def __init__(self):
        super().__init__()
        self.producer = Producer(self._config())

    def produce_json(self, topic: str, obj: dict, key: Optional[str] = None):
        self.ensure_topic(topic)
        data = json.dumps(obj).encode()
        self.producer.produce(topic=topic, key=key, value=data)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush(5)

class KafkaConsumerClient(KafkaBase):
    def __init__(self, group_id: str, topic: str, handler: Callable[[dict], None]):
        super().__init__()
        self.topic = topic
        self.handler = handler
        cfg = self._config()
        cfg.update({
            "group.id": group_id,
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
        })
        self._cfg = cfg
        self.consumer = Consumer(cfg)
        self.stop_flag = threading.Event()
        self.thread = None

    def start(self):
        self.consumer.subscribe([self.topic])
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        print(f"[Kafka] Consumer started on topic {self.topic}")

    def _loop(self):
        while not self.stop_flag.is_set():
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka] Consume error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode())
                self.handler(payload)
                # Only commit if auto-commit is disabled
                if not self._cfg.get("enable.auto.commit", False):
                    self.consumer.commit(msg, asynchronous=False)
            except Exception as e:
                try:
                    tp = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
                except Exception:
                    tp = "unknown"
                print(f"[Kafka] Handler error at {tp}: {e}")

    def stop(self):
        self.stop_flag.set()
        if self.thread:
            self.thread.join(timeout=5)
        self.consumer.close()