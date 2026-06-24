# Containerized Zero-ETL Data Mesh Pipeline

This repository implements a containerized real-time ingestion pipeline designed around a Zero-ETL Data Mesh architecture. By integrating Apache Kafka for message queuing, Apache Spark Structured Streaming for streaming computations, and Apache Iceberg for table layout management over AWS S3 storage, it establishes a high-performance local development sandbox orchestrated via Docker Compose.

---

## Architectural Objectives & Milestones

### 1. Core Streaming & Ingest Setup
- **Implementation (What we did)**: Provisioned a local Docker network hosting Apache Kafka and Apache Spark. Built a streaming Spark job (`kafka_to_iceberg.py`) that pulls events from the `users` topic, using transaction checkpointing to ensure end-to-end reliability and exactly-once processing.
- **Rationale (Why we did it)**: To establish a resilient, failure-tolerant ingestion layer capable of processing steady event streams with structural durability.

### 2. Lakehouse Table Layout via Apache Iceberg
- **Implementation (What we did)**: Configured AWS S3 object storage to host an Iceberg warehouse catalog. Coded initialization (`open_iceberg_shell.py`) and data query (`query_users.py`) actions. Conducted targeted record cleanups using standard transactional delete patterns on invalid rows.
- **Rationale (Why we did it)**: To transition from basic directory-based file storage to a structured, transactional table format that supports reliable schema validation, metadata management, and analytical query acceleration.

### 3. Optimizations: Partitioning, Schema Changes, and Streaming Deduplication
- **Implementation (What we did)**:
  - **Schema Changes**: Expanded the user record structure by adding timestamp tracking (`signup_ts`) and integer age fields (`age`) without pipeline downtime.
  - **Physical Clustering**: Partitioned records daily based on the signup timestamp (`days(signup_ts)`) and distributed them into 16 hash buckets by user ID (`bucket(16, id)`).
  - **Stateful Deduplication**: Built a streaming partition-window filter that resolves overlapping and late-arriving updates by primary key, merging only the newest state into S3.
- **Rationale (Why we did it)**:
  - **Schema Changes**: To maintain structural adaptability for evolving business requirements without requiring database migrations.
  - **Physical Clustering**: To accelerate query execution speeds by allowing analytical engines to skip irrelevant file folders during partition pruning.
  - **Stateful Deduplication**: To enforce data cleanliness, preventing duplicate stream payloads from polluting analytical reporting layers.

### 4. Storage Housekeeping & Expiration Utilities
- **Implementation (What we did)**: Created an automated housekeeping script (`expire_snapshots.py`) that purges outdated table snapshots, freeing up storage by deleting unreferenced manifest lists and underlying S3 data files.
- **Rationale (Why we did it)**: To manage AWS storage fees dynamically and enhance metadata access speeds by pruning old historical structures.

---

## Technologies Employed
- **Container Orchestration**: Docker Compose
- **Execution Engine**: Apache Spark 3.5.1
- **Ingestion & Queuing**: Apache Kafka
- **Table Management**: Apache Iceberg
- **Language**: Python 3
- **Storage Target**: AWS S3 (S3a connector)

---

## Directory Structure

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
