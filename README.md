# Flatten App with PySpark

This project provides a PySpark-based data pipeline to ingest, transform, and persist structured data extracted from nested raw formats into a PostgreSQL database.

## Prerequisites

- Docker and Docker Compose
- Python (for local development, optional)
- Git

## Setup Instructions

Follow the steps below to configure and run the application:

### 1. Clone the Repository

```bash
git clone https://github.com/danielfhenrique789/flatten-app-with-pyspark
```

### 2. Configure Your Environment

Create a file named config.json inside the config/ directory with the following structure:

```json
{
  "app_name": "Flatten App",
  "databases": {
    "postgres": {
      "url": "jdbc:postgresql://postgres-db:5432/mydatabase",
      "properties": {
        "user": "postgresuser",
        "password": "postgrespassword",
        "driver": "org.postgresql.Driver"
      }
    }
  },
  "jobs": {
    "LoadRawData": {
      "input_path": "data/mock_dataset.csv",
      "output_path": "data/bronze/raw.parquet"
    },
    "FlatteningAddress": {
      "input_path": "data/bronze/raw.parquet",
      "output_path": "data/silver/address.parquet"
    },
    "FlatteningTransactions": {
      "input_path": "data/bronze/raw.parquet",
      "output_path": "data/silver/transactions.parquet"
    },
    "SaveClient": {
      "schema_name": "client_schema",
      "fields": ["runtime_id", "names", "mail", "account_created_at"],
      "db": "postgres",
      "input_path": "data/bronze/raw.parquet",
      "table_name": "tb_client"
    },
    "SaveAddress": {
      "schema_name": "address_schema",
      "fields": "*",
      "db": "postgres",
      "input_path": "data/silver/address.parquet",
      "table_name": "tb_address"
    },
    "SaveTransactions": {
      "schema_name": "transaction_schema",
      "fields": "*",
      "db": "postgres",
      "input_path": "data/silver/transactions.parquet",
      "table_name": "tb_transaction"
    }
  }
}
```

###3. Run setup

Use the helper script to setup your environment:
```bash
./app.sh --setup
```
This script will:

- Create necessary folders
- Start the PostgreSQL container
- Run unit tests

### 4. Add the Input Dataset

Place the input mock file (e.g., mock_dataset.csv) in the data/ directory:
```bash
flatten-app-with-pyspark/
└── data/
    └── mock_dataset.csv
```


###5. Run the Pipeline

Use the helper script to execute the pipeline:
```bash
./app.sh --run_pipeline
```
This script will:
- Run all PySpark jobs in sequence
- Load the final data into PostgreSQL

###6. Postgres shell

Use the helper script to access the postgres container:
```bash
./app.sh --exec_db
```

## How to Add a New Job

This project supports modular and dynamically loaded Spark jobs. Follow the instructions below to create and configure a new job in the pipeline.

---

### Step 1: Create the Job File

Create a new Python file under the `jobs/` folder using **PEP8 snake_case naming**, e.g., `my_job_name.py`. Inside the file, define a class using **CamelCase** matching the filename, e.g., `MyJobName`.

**Template:**
```python
from base.spark_job import SparkJob

class MyJobName(SparkJob):

    def run(self, config):
        # Your code comes here.
        pass
```
### Step 2: Register the Job in config.json

Add a new entry under the jobs section with the class name as the key. Provide any paths or parameters needed.

```json
"jobs": {
  "MyJobName": {
    "input_path": "data/input.csv",
    "output_path": "data/output.parquet"
  }
}
```
###Step 3: Add a Save Job (Optional – to persist data)

To write data to PostgreSQL using an existing generic save job:
####1. Define a new schema in the schemas/ folder (e.g., new_table.py with new_schema inside).
####2. Add a configuration block to config.json for a job starting with Save....

```json
"jobs": {
  "SaveNewTable": {
    "schema_name": "new_schema",
    "fields": ["field1", "field2", "field3"],
    "db": "postgres",
    "input_path": "data/output.parquet",
    "table_name": "tb_new_table"
  }
}
```
⚠️ You do not need to create a new job file for SaveNewTable. The generic save logic handles it using the configuration above.

###Step 4: Run Your Job

To run a single job independently:
```bash
./app.sh --run MyJobName
```

###Step 5: Add Job to Pipeline (Optional)

If you’d like your job to run as part of the full pipeline, add the job name to the JOBS list in the script scripts/sh/run_pipeline.sh.

```bash
JOBS=(
  LoadRawData
  MyJobName
  SaveNewTable
)
```




## Architecture Overview

This project implements a modular, scalable data pipeline built with **Apache Spark** and **PostgreSQL**, containerized using **Docker**. The architecture is designed with clear separation of concerns, automation, and reusability in mind.

### Key Components

- **PySpark Jobs**: Modular jobs defined under `src/jobs/`, each extending a base `SparkJob` class. These jobs handle different pipeline stages such as loading raw data, flattening nested structures, and persisting results to a database.
- **PostgreSQL**: Used as the target relational database to persist cleaned and structured data.
- **Docker & Docker Compose**: The environment is containerized, enabling reproducible and isolated execution across systems.
- **Dynamic Job Configuration**: A centralized `config/config.json` allows job behavior to be configured without modifying code.
- **Unit Testing**: All core logic and transformation classes are thoroughly tested using `pytest`, with tests running inside Docker.

---

## Advantages

- **Separation of Concerns**: Each job is isolated and handles a single responsibility, improving maintainability and testability.
- **Scalability**: Built on Spark, the system is capable of scaling horizontally to handle large datasets efficiently.
- **Schema Validation**: Every transformation includes schema enforcement and validation, ensuring data quality and integrity.
- **Dynamic Class Loading**: Jobs are dynamically discovered and loaded based on naming conventions, reducing boilerplate.
- **Pipeline Flexibility**: The job runner script accepts dynamic job names, allowing developers to trigger specific stages as needed.
- **Automation Scripts**: Shell scripts handle environment setup, testing, and job orchestration, streamlining developer workflows.

---

## Best Practices Followed

- **Consistent Schema Definitions**: All schemas are declared explicitly in the `schemas/` directory, improving data contract management.
- **Error Handling**: Try-catch blocks wrap all job executions, with meaningful error messages for debugging.
- **Code Modularity**: Utility functions, configuration loading, and job dispatching are split into dedicated helper files.
- **Testing in Isolation**: Unit tests mock Spark behavior where needed and validate transformation logic without requiring live Spark execution.
- **Reusable Docker Image**: A single Dockerfile supports both production and test runs by using different Docker Compose services.

---

This design enables efficient development, debugging, and scaling of Spark-based data workflows, making it suitable for both local development and cloud deployment.

##Project Structure
```bash
.
├── config/             # Configuration files (JSON)
├── data/               # Input and intermediate datasets
├── docker/             # Dockerfile and Spark config
├── scripts/            # Shell scripts to manage jobs and setup
├── src/                # Source code: PySpark jobs and utilities
├── tests/              # Unit tests
├── compose-postgres.yml
├── docker-compose.yml
└── README.md
```

####Notes
	•	All database tables and schemas are managed by init.sql in scripts/sql/.
	•	Jobs are configured dynamically based on config.json.
	•	PostgreSQL is used for persistence; credentials and URLs are defined in the configuration file.




