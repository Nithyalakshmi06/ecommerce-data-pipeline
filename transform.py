from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_PATH = "s3a://ecommerce-raw/raw/"
PROCESSED_PATH = "s3a://ecommerce-raw/processed/"


def get_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceOrdersTransform")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
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
    spark.sparkContext.setLogLevel("WARN")  # quiet down Spark's very chatty default logs

    print(f"Reading raw events from {RAW_PATH} ...")
    raw_df = spark.read.json(RAW_PATH)

    print(f"Raw row count: {raw_df.count()}")
    raw_df.printSchema()

    # ---- Cleaning steps ----
    cleaned_df = (
        raw_df
        .dropDuplicates(["order_id"])                       # remove duplicate events
        .dropna(subset=["order_id", "product_id", "total_amount"])  # drop rows missing critical fields
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("event_date", F.to_date("event_timestamp"))  # derived column, useful for partitioning
    )

    print(f"Cleaned row count: {cleaned_df.count()}")

    # ---- Write out as partitioned Parquet ----
    (
        cleaned_df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(PROCESSED_PATH)
    )

    print(f"Done. Cleaned Parquet written to {PROCESSED_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
