import os
from dotenv import load_dotenv
from zeroetl.table_management import create_iceberg_table

# Load environment variables from .env file
load_dotenv()

def main():
    warehouse_path = os.getenv("AWS_BUCKET_NAME")
    if not warehouse_path:
        raise ValueError("AWS_BUCKET_NAME environment variable must be set in .env")

    # Ensure warehouse path format is correct (e.g. s3a://bucket-name/warehouse/)
    warehouse_path = warehouse_path.rstrip('/') + '/'

    # Configure the Iceberg table definition
    # - Schema includes signup_ts and age fields as required by data evolution
    # - Partitioning is daily by signup_ts and 16-bucket hash by id
    table_config = {
        "table_name": "mycatalog.db.users",
        "schema": "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT",
        "partition_spec": "days(signup_ts), bucket(16, id)",
        "location": f"{warehouse_path}db/users",
        "warehouse_path": warehouse_path,
        "spark_configs": {
            "spark.sql.shuffle.partitions": "4"
        }
    }

    print(f"Initializing Iceberg table schema: {table_config['table_name']}")
    print(f"Warehouse Location: {table_config['location']}")
    
    # Call the zeroetl utility to recreate/create the table
    create_iceberg_table(table_config)
    print("Iceberg table initialized successfully.")

if __name__ == "__main__":
    main()
