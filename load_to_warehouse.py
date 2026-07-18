from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
PROCESSED_PATH = "s3a://ecommerce-raw/processed/"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/ecommerce_dw"
POSTGRES_USER = "dwuser"
POSTGRES_PASSWORD = "dwpassword"
TABLE_NAME = "orders"


def get_spark_session():
    return (
        SparkSession.builder
        .appName("LoadToWarehouse")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading cleaned Parquet from {PROCESSED_PATH} ...")
    df = spark.read.parquet(PROCESSED_PATH)
    print(f"Row count to load: {df.count()}")

    print(f"Writing to Postgres table '{TABLE_NAME}' ...")
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", TABLE_NAME)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")   # replace the table each run, for this learning setup
        .save()
    )

    print(f"Done. Loaded into Postgres table '{TABLE_NAME}'.")
    spark.stop()


if __name__ == "__main__":
    main()
