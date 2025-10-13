import os, boto3

class S3Client:
    def __init__(self):
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.bucket = os.getenv("RAW_BUCKET")
        self.client = boto3.client("s3", region_name=self.region)

    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/json"):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def download_bytes(self, key: str):
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False