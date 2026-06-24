import os
from dotenv import load_dotenv
from zeroetl.table_management import compact_iceberg_table

# Load environment variables
load_dotenv()

def main():
    warehouse_path = os.getenv("AWS_BUCKET_NAME")
    if not warehouse_path:
        raise ValueError("AWS_BUCKET_NAME environment variable must be set in .env")

    warehouse_path = warehouse_path.rstrip('/') + '/'
    table_name = "mycatalog.db.users"

    # Configure the table optimization properties
    table_config = {
        "table_name": table_name,
        "warehouse_path": warehouse_path,
        "spark_configs": {
            "spark.sql.shuffle.partitions": "4"
        }
    }

    print(f"Starting Iceberg table optimization/compaction for: {table_name}")
    
    # Call the zeroetl utility to compact small files
    compact_iceberg_table(table_config)
    
    print("Iceberg table compaction query executed successfully.")

if __name__ == "__main__":
    main()
