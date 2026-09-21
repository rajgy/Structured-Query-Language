# PySpark SQL Execution & Local Database Integration

[![Python 3.x](https://img.shields.io/badge/Python-3.x-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Localhost-4169E1?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

A lightweight demonstration of executing distributed SQL queries using **PySpark SQL** and connecting PySpark pipelines directly to local relational databases via JDBC.

---

## Key Highlights & Architecture

* **Embedded SQL Execution:** Demonstrates running ANSI SQL queries directly against Spark DataFrames using `spark.sql()`.
* **Database Interoperability:** Implements JDBC connections to ingest data from and write results to a local PostgreSQL database instance.
* **Schema Management:** Handles explicit schema definitions, data type casting, and table aggregations in Apache Spark.

---

## Repository Structure

```text
Structured-Query-Language/
├── SQLPracticing.py            # PySpark SQL query execution and DataFrame operations
├── sqlPracticing_localhost.py  # PySpark to local PostgreSQL/MySQL database connection via JDBC
└── README.md                   # Project documentation



## Technical Overview

### 1. PySpark SQL Workflows (`SQLPracticing.py`)
* **SparkSession Management:** Initializes entry points for local distributed execution.
* **In-Memory Querying:** Registers DataFrames as temporary SQL views (`createOrReplaceTempView`) for ANSI SQL operations.
* **Transformations & Aggregations:** Demonstrates relational data filtering, grouping, and multi-field projections using Spark SQL syntax.

### 2. Localhost Database Pipeline (`sqlPracticing_localhost.py`)
* **Relational Storage Ingestion:** Connects PySpark directly to local database instances (PostgreSQL/MySQL) via JDBC driver.
* **Read/Write Operations:** Implements structured reads and configurable data output modes (`append`, `overwrite`).
* **Connection Security:** Externalizes database credentials and driver configurations for maintainable pipeline code.
