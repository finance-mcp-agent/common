import os, boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta

class DynamoClient:
    def __init__(self, table_name: str):
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.table_name = table_name
        self.table = boto3.resource("dynamodb", region_name=self.region).Table(table_name)

    def put(self, item: dict):
        self.table.put_item(Item=item)

    def get_by_id(self, id_: str):
        resp = self.table.scan(FilterExpression=Attr("id").eq(id_), Limit=1)
        items = resp.get("Items", [])
        return items[0] if items else None

    def scan_recent(self, minutes: int = 15, limit: int = 50):
        now = datetime.now(timezone.utc)
        start = (now - timedelta(minutes=minutes)).isoformat()
        filt = Attr("created_at").gt(start)
        resp = self.table.scan(FilterExpression=filt, Limit=limit)
        return resp.get("Items", [])

    def update_fields(self, id_: str, created_at: str, fields: dict):
        expr_names = {f"#k{i}": k for i, k in enumerate(fields.keys())}
        expr_vals = {f":v{i}": v for i, v in enumerate(fields.values())}
        update_expr = "SET " + ", ".join([f"{k}={v}" for k, v in zip(expr_names.keys(), expr_vals.keys())])
        self.table.update_item(
            Key={"id": id_, "created_at": created_at},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals
        )

    def recent_signals(self, minutes: int = 15, limit: int = 50) -> list[dict]:
        """
        For minimal changes, scan by created_at window.
        (Later you can add a GSI on ts only, to query instead of scan.)
        """
        now = datetime.now(timezone.utc)
        start = (now - timedelta(minutes=minutes)).isoformat()
        end = now.isoformat()
        filt = Attr("created_at").between(start, end)
        items = []
        resp = self.table.scan(FilterExpression=filt, Limit=limit)
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp and len(items) < limit:
            resp = self.table.scan(FilterExpression=filt, Limit=limit - len(items),
                                  ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        return items