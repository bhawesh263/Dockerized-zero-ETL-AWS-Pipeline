# Zero ETL Data Mesh – Project Overview

This project establishes a foundational streaming data pipeline, creating a Zero-ETL Data Mesh architecture. It leverages Kafka for high-throughput messaging, Apache Spark Structured Streaming for real-time data processing, and Apache Iceberg for robust, high-performance table format management on AWS S3. All components are orchestrated using Docker Compose for a streamlined local development environment.

## Project Goals

### 1. Foundational Setup & Streaming Ingestion
- **What we did**: Established a Dockerized environment for Kafka and Spark. Configured a Kafka topic (`users`) and built a Spark Structured Streaming job (`kafka_to_iceberg.py`) to ingest data from this topic. Enabled checkpointing for fault tolerance and exactly-once processing guarantees.
- **Why we did it**: To create a scalable and resilient core infrastructure for real-time data ingestion, capable of handling continuous data streams reliably.

### 2. Apache Iceberg Integration & Basic Data Management
- **What we did**: Integrated Apache Iceberg as the table format layer. Configured AWS S3 as the Iceberg warehouse, managing environment variables for secure access. Developed Spark jobs to initialize (`open_iceberg_shell.py`) and query (`query_users.py`) the Iceberg table. Demonstrated basic data management by performing DELETE operations on invalid rows.
- **Why we did it**: To introduce a powerful table format that enables schema evolution, time travel, and robust data management features directly on object storage, moving beyond basic file storage.

### 3. Data Product Refinement – Schema Evolution, Partitioning & Deduplication
- **What we did**:
  - **Schema Evolution**: Evolved the `users` table schema by adding new fields (`signup_ts`, `age`) to accommodate richer user data.
  - **Partitioning**: Implemented partitioning by `days(signup_ts)` and `bucket(16, id)` to physically organize data on S3.
  - **Deduplication**: Developed a robust streaming deduplication strategy to ensure uniqueness of records based on `id`, always reflecting the latest data.
- **Why we did it**:
  - **Schema Evolution**: To maintain agility as data requirements change, allowing schema updates without downtime or complex data migrations.
  - **Partitioning**: To significantly boost query performance for analytical workloads by enabling data pruning at the storage layer.
  - **Deduplication**: To ensure high data quality and consistency in a streaming environment, preventing duplicate records from polluting analytical datasets.

### 4. Data Retention & Snapshot Expiration
- **What we did**: Developed and executed a script (`expire_snapshots.py`) to remove old snapshots from the Iceberg table, successfully removing old snapshots and their associated data files, manifest lists, and manifest files.
- **Why we did it**: To manage storage costs on S3 by periodically cleaning up old data files that are no longer referenced by a valid snapshot, improving query performance by reducing the size of the table's metadata.

---

## Technologies Used
- Docker Compose
- Apache Spark 3.5.1
- Apache Kafka
- Apache Iceberg
- Python 3
- S3-compatible storage (AWS S3)

---

## Project File Structure

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
