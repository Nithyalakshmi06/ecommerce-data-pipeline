# Real-Time E-Commerce Data Pipeline

A complete, working data engineering pipeline built from scratch — simulating real-time
order events and moving them through ingestion, raw storage, transformation, orchestration,
and a queryable analytics warehouse. Built entirely with free, local, open-source tools that
map directly to their AWS equivalents.

## Architecture
| Local tool | AWS equivalent |
|---|---|
| Kafka | Kinesis Data Streams |
| MinIO | S3 |
| PySpark | AWS Glue |
| Postgres | Redshift |
| Airflow | MWAA / Step Functions |

## Why this design

- **Streaming ingestion** decouples event producers from consumers and absorbs traffic spikes.
- **Raw zone kept untouched** so data can always be reprocessed if a transform bug is found later.
- **Date-partitioned storage** lets query engines skip irrelevant data instead of scanning everything.
- **Parquet** for processed data — columnar, compressed, faster to query than JSON/CSV.
- **Airflow pre-flight check** (`check_infra`) verifies MinIO is reachable before running the Spark
  job, so failures surface in seconds instead of a silent hang.

## Prerequisites
- Ubuntu with Docker and Docker Compose
- Python 3.10+, Java 17 (for PySpark)

## Setup and run

```bash
docker compose up -d
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
chmod +x setup_kafka_topic.sh && ./setup_kafka_topic.sh

# Terminal 1
python producer/generate_events.py

# Terminal 2
python consumer/kafka_to_minio.py

# Once data has landed in MinIO:
python spark_jobs/transform.py
python spark_jobs/load_to_warehouse.py
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin)

<img width="1910" height="1002" alt="Screenshot from 2026-07-16 18-45-58" src="https://github.com/user-attachments/assets/3a80bf6a-3aa5-4e64-b419-9da50da46c5d" />


Airflow UI: http://localhost:8080 (after running `airflow standalone`)

<img width="1920" height="1080" alt="Screenshot from 2026-07-18 10-10-23" src="https://github.com/user-attachments/assets/ee58c3c0-a34f-4f7b-97d1-af7386e10cec" />



## What I learned building this

- Debugging a hung Airflow task back to stopped Docker containers, not application code
- Fixing a Postgres `ROUND()` type-cast error (`double precision` vs `numeric`)
- Adding a fail-fast infrastructure health check instead of a silent 15+ minute hang
- Partition design and why raw/processed separation matters at scale

## Next steps
- Deploy briefly to real AWS (Kinesis, S3, Glue, Redshift) via Terraform, using free-tier credits
- Add data quality checks as a separate Airflow task
