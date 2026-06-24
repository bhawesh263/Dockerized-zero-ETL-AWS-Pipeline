import os
from dotenv import load_dotenv
from zeroetl.table_management import expire_iceberg_snapshots

# Load environment variables
load_dotenv()

def main():
    warehouse_path = os.getenv("AWS_BUCKET_NAME")
    if not warehouse_path:
        raise ValueError("AWS_BUCKET_NAME environment variable must be set in .env")

    warehouse_path = warehouse_path.rstrip('/') + '/'
    table_name = "mycatalog.db.users"

    # Configure the table snapshot expiration properties
    # - Expire snapshots older than 30 seconds by default for quick test iteration
    table_config = {
        "table_name": table_name,
        "warehouse_path": warehouse_path,
        "older_than_seconds": 30,
        "spark_configs": {
            "spark.sql.shuffle.partitions": "4"
        }
    }

    print(f"Starting snapshot expiration for: {table_name}")
    
    # Call the zeroetl utility to expire older snapshots
    expire_iceberg_snapshots(table_config)
    
    print("Snapshot expiration query executed successfully.")

if __name__ == "__main__":
    main()
