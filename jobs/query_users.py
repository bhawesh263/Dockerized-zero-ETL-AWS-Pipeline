import os
from dotenv import load_dotenv
from zeroetl.table_management import query_iceberg_table, get_spark_session

# Load environment variables
load_dotenv()

def main():
    warehouse_path = os.getenv("AWS_BUCKET_NAME")
    if not warehouse_path:
        raise ValueError("AWS_BUCKET_NAME environment variable must be set in .env")

    warehouse_path = warehouse_path.rstrip('/') + '/'
    table_name = "mycatalog.db.users"

    table_config = {
        "table_name": table_name,
        "warehouse_path": warehouse_path,
        "spark_configs": {
            "spark.sql.shuffle.partitions": "4"
        },
        "query_type": "latest"
    }

    print("--- Querying Iceberg Table (Current State) ---")
    query_iceberg_table(table_config)

    # Establish a Spark Session to run transactional DELETE commands
    print("\n--- Running Data Management / Refinement ---")
    spark = get_spark_session(
        app_name="DataRefinement",
        warehouse_path=warehouse_path
    )
    
    try:
        # Goal 2: Demonstrated basic data management by performing DELETE operations on invalid rows.
        # Check count of rows where age is negative or invalid
        print("Checking for invalid user profiles (age < 0)...")
        invalid_df = spark.sql(f"SELECT * FROM {table_name} WHERE age < 0")
        invalid_df.show(truncate=False)
        invalid_count = invalid_df.count()

        if invalid_count > 0:
            print(f"Found {invalid_count} invalid records. Executing DELETE...")
            spark.sql(f"DELETE FROM {table_name} WHERE age < 0")
            print("Invalid records deleted successfully.")
        else:
            print("No invalid records found.")

    except Exception as e:
        print(f"Error executing data refinement: {e}")
    finally:
        spark.stop()

    print("\n--- Querying Iceberg Table (Post-Refinement State) ---")
    query_iceberg_table(table_config)

if __name__ == "__main__":
    main()
