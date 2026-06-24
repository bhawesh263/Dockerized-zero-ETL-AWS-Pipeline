import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, row_number, desc
from pyspark.sql.window import Window
from zeroetl.ingestion import get_spark_session

# Load environment variables
load_dotenv()

def upsert_to_iceberg(batch_df, batch_id):
    """
    Idempotent MERGE operation executed on each micro-batch.
    Performs intra-batch deduplication to keep only the latest record per id,
    and then merges it into the target Iceberg table.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty. Skipping.")
        return

    print(f"--- Processing Batch {batch_id} with {batch_df.count()} records ---")

    # 1. Intra-batch Deduplication
    # If the micro-batch contains multiple records for the same id, keep the one with the latest signup_ts
    window_spec = Window.partitionBy("id").orderBy(desc("signup_ts"))
    deduplicated_batch = (
        batch_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # 2. Register the batch as a temporary view for SQL merging
    deduplicated_batch.createOrReplaceTempView("batch_updates")

    # 3. Inter-batch Merge query
    # Match on primary key (id). Only update if the incoming record is newer than the existing record.
    merge_query = """
        MERGE INTO mycatalog.db.users t
        USING batch_updates s
        ON t.id = s.id
        WHEN MATCHED AND s.signup_ts > t.signup_ts THEN
            UPDATE SET 
                t.name = s.name, 
                t.email = s.email, 
                t.signup_ts = s.signup_ts, 
                t.age = s.age
        WHEN NOT MATCHED THEN
            INSERT (id, name, email, signup_ts, age) 
            VALUES (s.id, s.name, s.email, s.signup_ts, s.age)
    """

    print(f"Executing MERGE INTO for batch {batch_id}...")
    batch_df.sparkSession.sql(merge_query)
    print(f"Batch {batch_id} merged successfully.")

def main():
    warehouse_path = os.getenv("AWS_BUCKET_NAME")
    if not warehouse_path:
        raise ValueError("AWS_BUCKET_NAME environment variable must be set in .env")
    
    warehouse_path = warehouse_path.rstrip('/') + '/'
    checkpoint_location = f"{warehouse_path}checkpoints/users_stream"

    # Resolve bootstrap servers address (localhost mapping to internal network service name if required)
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    if "localhost" in kafka_bootstrap_servers:
        kafka_bootstrap_servers = "kafka:29092"

    print(f"Starting Spark Structured Streaming. Listening to Kafka: {kafka_bootstrap_servers}")
    print(f"Checkpoint location: {checkpoint_location}")

    # Create SparkSession with Iceberg and AWS dependencies configured
    spark = get_spark_session(
        app_name="ZeroETLKafkaToIcebergStreaming",
        warehouse_path=warehouse_path
    )
    spark.sparkContext.setLogLevel("WARN")

    # JSON schema definition for the incoming Kafka payload
    # Note: signup_ts is parsed as string and then cast to timestamp for formatting consistency
    kafka_value_schema = StructType() \
        .add("id", StringType(), True) \
        .add("name", StringType(), True) \
        .add("email", StringType(), True) \
        .add("signup_ts", StringType(), True) \
        .add("age", IntegerType(), True)

    # Read the streaming source from Kafka
    streaming_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "users")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse and format the binary Kafka stream values
    parsed_streaming_df = (
        streaming_raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), kafka_value_schema).alias("data"))
        .select("data.*")
        .withColumn("signup_ts", to_timestamp(col("signup_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    )

    # Configure Structured Streaming write sink with foreachBatch for merge support
    query = (
        parsed_streaming_df.writeStream
        .foreachBatch(upsert_to_iceberg)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    print("Streaming query started. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
