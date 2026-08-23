# FlowBridge Snowflake Data Platform

> **End-to-end cloud data engineering platform for supply-chain analytics using Azure, Snowflake, Python, SQL, and Streamlit.**

## Overview

FlowBridge International Ltd. operates a supply-chain business that generates operational data across orders, customers, products, suppliers, warehouses, shipments, and inventory.

This project implements a **production-style cloud data platform** designed to ingest operational data, transform it through a Snowflake Medallion Architecture, apply data quality and governance controls, and expose trusted business metrics through a Streamlit analytics dashboard.

The platform demonstrates an end-to-end data engineering lifecycle:

**Data Generation → Cloud Ingestion → Snowflake Processing → Data Modeling → Governance → Analytics**

---

## Business Problem

Supply-chain operations require reliable and timely visibility into:

* Order volumes and revenue
* Product performance
* Supplier performance
* Warehouse operations
* Inventory activity
* Shipment performance
* Customer activity
* Operational trends

Raw operational data is difficult to analyze directly because it can contain inconsistent structures, duplicate records, incomplete attributes, and operational fields that are not optimized for analytics.

The FlowBridge platform addresses this by creating a governed analytical layer that converts raw operational data into trusted, business-ready datasets.

---

## Architecture

```text
                         FLOWBRIDGE DATA PLATFORM

 ┌──────────────────────┐
 │ Python + Faker       │
 │ Nested JSON Data     │
 │ Generation           │
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ Azure ADLS Gen2      │
 │ Cloud Data Lake      │
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ Azure Event Grid     │
 │ + Storage Queue      │
 │ Event-driven Flow    │
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ Snowpipe             │
 │ Automated Ingestion  │
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────────────────────┐
 │              SNOWFLAKE               │
 │                                      │
 │  ┌──────────────┐                    │
 │  │    BRONZE    │ Raw ingestion      │
 │  └──────┬───────┘                    │
 │         ▼                            │
 │  ┌──────────────┐                    │
 │  │    SILVER    │ Clean + transform │
 │  └──────┬───────┘                    │
 │         ▼                            │
 │  ┌──────────────┐                    │
 │  │     GOLD     │ Business models    │
 │  └──────┬───────┘                    │
 │         ▼                            │
 │  ┌──────────────┐                    │
 │  │   SERVING    │ Secure analytics   │
 │  └──────────────┘                    │
 └───────────────────┬──────────────────┘
                     │
                     ▼
             ┌──────────────────┐
             │ Streamlit        │
             │ Analytics        │
             │ Dashboard        │
             └──────────────────┘
```

---

## Technology Stack

| Area                | Technology                              |
| ------------------- | --------------------------------------- |
| Programming         | Python                                  |
| Data Generation     | Faker                                   |
| Cloud Storage       | Azure ADLS Gen2                         |
| Event Processing    | Azure Event Grid                        |
| Messaging           | Azure Storage Queue                     |
| Data Ingestion      | Snowpipe                                |
| Data Platform       | Snowflake                               |
| Transformation      | Snowflake SQL                           |
| Processing          | Streams & Tasks                         |
| Automation          | Stored Procedures                       |
| Analytical Modeling | Star Schema                             |
| History Management  | SCD Type 1                              |
| Analytics           | Streamlit                               |
| Security            | Snowflake RBAC                          |
| Governance          | Secure Views, roles and access controls |
| Deployment          | DEV → PROD                              |

---

## Medallion Architecture

### Bronze Layer

The Bronze layer preserves data close to its original ingestion structure.

Responsibilities include:

* Raw data ingestion
* Source-system preservation
* Initial metadata capture
* Ingestion timestamps
* Source traceability

The Bronze layer provides an auditable foundation for downstream processing.

### Silver Layer

The Silver layer transforms raw data into clean, standardized datasets.

Processing includes:

* Data type standardization
* Deduplication
* Data cleansing
* Null handling
* Business-rule validation
* Relational normalization
* Transformation of nested operational structures

### Gold Layer

The Gold layer provides business-oriented analytical models.

It contains dimensional and fact structures designed for analytical workloads, including:

* Fact tables
* Customer dimensions
* Product dimensions
* Supplier dimensions
* Warehouse dimensions
* Shipment-related analytical structures

The analytical model follows a **star-schema approach** to simplify reporting and improve query usability.

### Serving Layer

The Serving layer exposes curated data for analytics and consumption.

It includes:

* Secure views
* Business-ready metrics
* Reporting datasets
* Dashboard-oriented data structures

---

## Snowflake Engineering

The project demonstrates several Snowflake-native data engineering capabilities:

### Streams

Snowflake Streams are used to track changes in source datasets and support incremental processing.

### Tasks

Snowflake Tasks orchestrate recurring transformation workflows and provide automated movement of data between processing layers.

### Stored Procedures

Stored procedures encapsulate reusable transformation and orchestration logic.

### Dynamic Tables

Dynamic Tables support declarative transformation pipelines and continuously maintained analytical datasets.

### Star Schema

The Gold layer uses fact and dimension modeling to provide a business-friendly analytical structure.

### Surrogate Keys

Dimension tables use surrogate keys to separate analytical identities from operational source identifiers.

### SCD Type 1

Dimension updates are handled using a Type 1 approach where current attribute values are maintained without preserving historical versions.

---

## Data Quality

Data quality is treated as part of the pipeline rather than as a downstream reporting concern.

The platform incorporates validation around:

* Required fields
* Data types
* Duplicate records
* Referential relationships
* Business-rule consistency
* Transformation integrity
* Pipeline verification

Validation and verification SQL is included in:

```text
sql/09_resume_and_verify.sql
```

---

## Security and Governance

The platform includes Snowflake governance practices designed for controlled analytical access.

Key areas include:

* Role-based access control
* Environment separation
* Secure views
* Controlled object access
* Least-privilege principles
* Data consumption through curated serving objects

Governance documentation is available in:

```text
docs/governance.md
```

---

## DEV → PROD Deployment

The project separates development and production environments to support controlled deployment.

The deployment workflow covers:

```text
Development
     │
     ▼
Validation
     │
     ▼
Deployment
     │
     ▼
Production
     │
     ▼
Verification
```

Deployment documentation is available in:

```text
docs/deployment.md
```

---

## Analytics Dashboard

The platform includes a native Snowflake Streamlit dashboard designed to provide operational visibility.

The dashboard focuses on metrics such as:

* Total orders
* Revenue
* Customers
* Products
* Suppliers
* Daily order trends
* Revenue trends
* Operational breakdowns
* Supplier performance

The dashboard consumes curated analytical data rather than raw ingestion tables.

---

## Repository Structure

```text
FLOWBRIDGE-SNOWFLAKE-PROJECT/
│
├── docs/
│   ├── architecture.md
│   ├── business-problem.md
│   ├── data-model.md
│   ├── deployment.md
│   └── governance.md
│
├── sql/
│   ├── 01-account-setup.sql
│   ├── 02_bronze.sql
│   ├── 03_silver_stage1.sql
│   ├── 04_silver_stage2.sql
│   ├── 05_gold.sql
│   ├── 06_serving.sql
│   ├── 07_governance.sql
│   ├── 08_dev_to_prod.sql
│   └── 09_resume_and_verify.sql
│
├── .gitignore
└── README.md
```

---

## SQL Deployment Sequence

The SQL scripts are organized according to the platform lifecycle:

```text
01 → Account & Environment Setup
02 → Bronze Layer
03 → Silver Stage 1
04 → Silver Stage 2
05 → Gold Layer
06 → Serving Layer
07 → Governance
08 → DEV → PROD Deployment
09 → Resume & Verification
```

This ordering makes the project easier to reproduce and understand.

---

## Engineering Highlights

This project demonstrates practical experience with:

* End-to-end data platform architecture
* Cloud data ingestion
* Event-driven ingestion patterns
* Snowflake data engineering
* Medallion Architecture
* Incremental processing
* Streams and Tasks
* Stored Procedures
* Dynamic Tables
* Dimensional modeling
* Star schemas
* SCD Type 1
* Data quality
* Data governance
* RBAC
* Secure data serving
* DEV/PROD environment management
* Analytical dashboards

---

## Project Documentation

Detailed project documentation is available in the `docs/` directory:

* [Business Problem](docs/business-problem.md)
* [Architecture](docs/architecture.md)
* [Data Model](docs/data-model.md)
* [Deployment](docs/deployment.md)
* [Governance](docs/governance.md)

---

## Project Objective

The objective of FlowBridge is to demonstrate how a modern data engineer can design and implement a complete cloud analytics platform rather than simply write isolated SQL transformations.

The project emphasizes:

**Architecture → Reliability → Data Quality → Governance → Analytics**

---

## Author

**Viradidi**

Data Engineer · Analytics Engineer · Cloud Engineer

GitHub: [github.com/viradidi](https://github.com/viradidi)

Portfolio: [viradidi.github.io](https://viradidi.github.io)
