# Project DataWave – Karachi Electric (FYP)

A scalable, distributed data platform built for real-time analytics and fault prediction at Karachi Electric. Project DataWave leverages a modern lakehouse architecture to process over 120 million records from KE’s enterprise systems and surface insights via interactive dashboards and predictive models.

---

## Key Features

- **Distributed ETL Pipeline**  
  Built using **Apache Spark** and **Apache Iceberg**, capable of processing, transforming, and loading **120M+ records (~80GB)** with high efficiency and fault tolerance.

- **Incremental & Batch Processing**  
  Supports both full refreshes and incremental updates using **partitioned tables** and **timestamp-based ingestion**, with snapshot versioning via Iceberg.

- **Data Lakehouse Integration**  
  Designed on a modern **Lakehouse architecture** supporting **ACID transactions**, **schema evolution**, and scalable SQL queries.

- **Analytics-Ready Tables**  
  Data modeled for BI and ML workloads with partition pruning, vectorized I/O, and Parquet-based storage for fast reads.

- **Multi-Source Ingestion**  
  Unified structured and semi-structured data from KE’s **CRM**, **OMS**, and **SAP Billing** systems into a centralized lakehouse.

- **Forecasting and Predictions**  
  Trained **LSTM and neural network models** for fault prediction. Achieved an **F1 score of 89%** on real feeder data to forecast outages and reduce downtime.

- **KPI Analysis**  
  Generated and tracked **80+ KPIs**, including outage frequency, downtime, billing cycle anomalies, and resolution time across regional and temporal dimensions.

- **Dashboard Set-up**  
  Built a **9-tab interactive dashboard** in **Apache Superset** to visualize KPIs, monitor trends, and support operational decisions with filterable and drill-down views.

- **Secure & Auditable**  
  Integrated robust **data validation**, **logging**, and **schema enforcement** to ensure traceable and compliant data operations.

---

## Technologies Used

| Category | Tool/Technology |
|----------|-----------------|
| Distributed Processing | Apache Spark |
| Lakehouse Table Format | Apache Iceberg |
| Catalog Management | Nessie |
| SQL Query Layer | Dremio |
| Storage | MinIO (Parquet format) |
| Orchestration & Logic | Python, PySpark |
| Visualization | Apache Superset |
| Containerization | Docker |
| Notebooks & Modeling | Jupyter, LSTM |
| Version Control | Git |

---

## System Architecture
```mermaid
flowchart TD

%% Development Environment %%
subgraph DEV [Development Environment]
    GIT["Git – Version Control"]
    JN["Jupyter Notebooks – EDA & Modeling"]
    PY["Python / PySpark"]
    DOCKER["Docker – Containerization"]
end

%% ETL & Storage Layer %%
subgraph INGEST [ETL & Storage Layer]
    SPARK["Apache Spark – ETL Engine"]
    CLEAN["Data Cleaning & Transformation"]
    ICEBERG["Apache Iceberg – Lakehouse Format"]
    MINIO["MinIO – S3-Compatible Parquet Storage"]
    NESSIE["Nessie – Catalog Versioning"]
end

%% Modeling Layer %%
subgraph MODEL [Analytics & Modeling Layer]
    KPI["80+ KPI Calculations"]
    LSTM["LSTM – Fault Prediction Model"]
end

%% BI Layer %%
subgraph ANALYTICS [Query + BI Layer]
    DREMIO["Dremio – SQL Query Engine"]
    SUPERSET["Apache Superset – Dashboards"]
end

%% Connections %%
GIT --> PY
PY --> DOCKER
DOCKER --> SPARK
JN --> CLEAN
JN --> LSTM

SPARK --> CLEAN
CLEAN --> ICEBERG
ICEBERG --> MINIO
MINIO --> NESSIE
NESSIE --> DREMIO

CLEAN --> KPI
KPI --> SUPERSET
LSTM --> SUPERSET

DREMIO --> SUPERSET
