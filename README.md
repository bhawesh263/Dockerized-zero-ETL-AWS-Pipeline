# Dockerized Zero-ETL Data Mesh AWS Pipeline

This repository establishes a foundational streaming data pipeline creating a Zero-ETL Data Mesh architecture. It leverages Kafka for high-throughput messaging, Apache Spark Structured Streaming for real-time data processing, and Apache Iceberg for robust table format management on AWS S3. All components are orchestrated using Docker Compose for a streamlined local development environment.

## Architecture & Components

- **Apache Kafka (KRaft mode)**: Ingests stream messages under the `users` topic.
- **Apache Spark 3.5.1**: Streams records from Kafka, applies deduplication, and commits writes directly to the Apache Iceberg catalog on AWS S3.
- **Apache Iceberg**: High-performance table format enabling Daily partitioning by `signup_ts`, bucket-hash partitioning by `id`, schema evolution, compaction, and snapshot expiration.
- **zeroetl (PyPI)**: A utility python package mapping standard API operations for table initialization, batch ingestion, query, optimization, and snapshot expiration.

---

## File Structure

```plaintext
.
├── jobs/
│   ├── kafka_producer.py         # Produces sample user records to Kafka topic
│   ├── kafka_to_iceberg.py       # Spark Structured Streaming consumer with MERGE deduplication
│   ├── open_iceberg_shell.py     # Initializes/recreates the partitioned Iceberg table
│   ├── query_users.py            # Queries the Iceberg table & deletes invalid records (age < 0)
│   ├── optimize_iceberg_table.py # Compacts small files in the Iceberg table
│   └── expire_snapshots.py       # Expires old snapshots and orphaned S3 data files
├── configs/
│   └── spark-defaults.conf       # Spark defaults setting package and extension bindings
├── docker-compose.yaml           # Docker orchestrator for Spark Cluster & Kafka Broker
├── Dockerfile                    # Spark execution base container with zeroetl library
├── .env                          # Configuration details for AWS credentials & Kafka bootstrap servers
└── README.md                     # Project documentation
```

---

## Setup & Execution Instructions

### 1. Configure Credentials
Create a `.env` file in the root directory (copied from the template) and input your AWS S3 bucket and credentials:
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_BUCKET_NAME=s3a://your-iceberg-warehouse-bucket/
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 2. Boot Infrastructure
Launch the multi-container Docker compose stack:
```bash
docker compose up --build -d
```
Verify all containers (`kafka`, `spark-master`, `spark-worker`) are running:
```bash
docker compose ps
```

### 3. Initialize the Iceberg Table
Run the schema setup script inside the Spark driver container:
```bash
docker exec -it spark-master python jobs/open_iceberg_shell.py
```

### 4. Run the Spark Streaming Consumer
Start the streaming consumer job that listens to Kafka and performs stateful `MERGE INTO` operations on the S3 Iceberg table:
```bash
docker exec -it spark-master python jobs/kafka_to_iceberg.py
```
*(Leave this running in a terminal or run in background)*

### 5. Generate Kafka Events
You can run the mock event producer script either **inside the Docker container** (recommended, zero setup) or **on your host machine**:

**Option A: Run inside Docker container (No host setup required)**
```bash
docker exec -it spark-master python jobs/kafka_producer.py
```

**Option B: Run from the host machine**
First install the client packages on your host machine:
```bash
pip install zeroetl kafka-python-ng python-dotenv
```
Then run the producer:
```bash
python jobs/kafka_producer.py
```
This produces a batch of user profiles, including duplicate events (with older/newer timestamps) and invalid records.

### 6. Query Table and Refine Data Product
Inspect the records and run manual data correction (deleting records with negative ages):
```bash
docker exec -it spark-master python jobs/query_users.py
```

### 7. Optimize Table Layout (Compaction)
Merge small files into larger, optimized layout configurations:
```bash
docker exec -it spark-master python jobs/optimize_iceberg_table.py
```

### 8. Expire Old Snapshots
Clean up historical checkpoints and orphaned S3 data files to minimize storage costs:
```bash
docker exec -it spark-master python jobs/expire_snapshots.py
```

---

## Technical Features Demonstrated

1. **Exactly-Once Semantics**: Handled via Spark Structured Streaming checkpointing to the S3 warehouse bucket.
2. **Streaming Deduplication**: Uses `foreachBatch` to perform intra-batch window ordering and inter-batch `MERGE INTO` SQL commands based on primary key `id` and ordering timestamp `signup_ts`.
3. **Partition Pruning**: The table is physically organized by `days(signup_ts)` and bucket-hashed by `bucket(16, id)`.
4. **Data Management**: Showcases ACID transactional capability via `DELETE` statements on table formats over S3 object stores.
