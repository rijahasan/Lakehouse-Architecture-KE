# Distributed Data Engineering - Karachi Electric (Final Year Project)

## Overview

This project was developed in collaboration with **Karachi Electric (KE)** as part of our Final Year Project at Habib University. The goal was to design and implement a **robust, automated, scalable, and distributed data pipeline** capable of handling high-volume energy usage and operations data, enabling advanced analytics, operational insights, and data-backed decision-making. 

## Key Features

- **Distributed ETL Pipeline**: Built using **Apache Spark** and **Apache Iceberg**, capable of processing, transforming, and loading **120+ million records** efficiently.
- **Incremental & Batch Processing**: Supports both full data refreshes and incremental updates via partitioned and time-aware ingestion logic.
- **Data Lakehouse Integration**: Designed on a modern **Lakehouse architecture**, supporting schema evolution, ACID transactions, and performant queries at scale.
- **Analytics-Ready Tables**: Output datasets are modeled and optimized for analytics and machine learning workloads.
- **Multi-Source Ingestion**: Integrates structured and semi-structured data from multiple KE platforms.
- **Secure & Auditable**: Includes robust data validation, logging, and error-handling features.

## Technologies Used

- **Apache Spark** – Distributed data processing
- **Apache Iceberg** – Lakehouse table format with versioned datasets
- **Nessie*** - Catalog to handle iceberg tables
- **Dremio** – Query Platform
- **MinIO** – Columnar data storage (Parquet)
- **Python & PySpark** – Pipeline orchestration and transformation logic
- **Docker** – Containerized development and deployment
- **Git** – Version control and collaboration
- **Jupyter Notebooks** – Exploratory data analysis (EDA)
- **Apache Superset** - Dashboards

## System Architecture
```mermaid
flowchart TD
%%{init: {'dark':'true'}}%%

subgraph Dev["Development Environment"]
    GIT[Git]
    JN[Jupyter Notebooks]
    PY[Python & PySpark]
    DOCKER[Docker]
end

subgraph ETL["ETL Pipeline"]
    SPARK[Apache Spark]
    MINIO[MinIO Parquet Storage]
    ICEBERG[Apache Iceberg]
    NESSIE[Nessie Catalog]
end

subgraph Analytics["Analytics & Query Layer"]
    DREMIO[Dremio Query Platform]
    SUPERSET[Superset Dashboards]
end

GIT --> PY
JN --> PY
PY --> DOCKER
DOCKER --> SPARK
SPARK --> ICEBERG
ICEBERG --> MINIO
MINIO --> NESSIE
NESSIE --> DREMIO
DREMIO --> SUPERSET


