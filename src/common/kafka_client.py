import os, json, time, threading
from typing import Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import boto3

# export KAFKA_BOOTSTRAP="b-1.trader-msk.xxxxxx.kafka.us-east-1.amazonaws.com:9096,b-2..."
class KafkaBase:
    def __init__(self):
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.bootstrap = os.getenv("KAFKA_BOOTSTRAP", "")
        if not self.bootstrap:
            self.bootstrap = self._lookup_bootstrap()
        self.username, self.password = self._load_scram()

    def _lookup_bootstrap(self) -> str:
        ssm = boto3.client("ssm", region_name=self.region)
        return ssm.get_parameter(Name="/trader/dev/kafka_bootstrap")["Parameter"]["Value"]

    def _load_scram(self):
        sm = boto3.client("secretsmanager", region_name=self.region)
        sec = sm.get_secret_value(SecretId="AmazonMSK_trader_dev_kafka_scram")["SecretString"]
        creds = json.loads(sec)
        return creds["username"], creds["password"]

    def _config(self):
        return {
            "bootstrap.servers": self.bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": self.username,
            "sasl.password": self.password,
        }

    def ensure_topic(self, topic: str, partitions: int = 3, replication_factor: int = 2):
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
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
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
                self.consumer.commit(msg, asynchronous=False)
            except Exception as e:
                print(f"[Kafka] Handler error: {e}")

    def stop(self):
        self.stop_flag.set()
        if self.thread:
            self.thread.join(timeout=5)
        self.consumer.close()