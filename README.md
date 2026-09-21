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
