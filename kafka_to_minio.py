import json
import time
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from kafka import KafkaConsumer

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "ecommerce-raw"

BATCH_SIZE = 10
BATCH_TIMEOUT_SEC = 15


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client):
    existing = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")


def build_partition_key():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%S")
    return f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/orders_{ts}.json"


def flush_batch(s3_client, batch):
    if not batch:
        return
    key = build_partition_key()
    body = "\n".join(json.dumps(event) for event in batch)
    s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"  -> Flushed {len(batch)} events to s3://{MINIO_BUCKET}/{key}")


def main():
    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client)

    consumer = KafkaConsumer(
        "orders",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="raw-landing-consumer",
    )

    print("Consumer started. Reading 'orders', landing into MinIO. Ctrl+C to stop.\n")
    batch = []
    last_flush = time.time()

    try:
        for message in consumer:
            batch.append(message.value)
            print(f"Received: {message.value['order_id']}")
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_TIMEOUT_SEC:
                flush_batch(s3_client, batch)
                batch = []
                last_flush = time.time()
    except KeyboardInterrupt:
        print("\nStopping, flushing remaining events...")
        flush_batch(s3_client, batch)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
