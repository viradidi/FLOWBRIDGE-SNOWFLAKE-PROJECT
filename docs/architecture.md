# FlowBridge Data Platform Architecture

## 1. Overview

FlowBridge is a cloud-based supply-chain data platform implemented with Azure and Snowflake.

The platform follows a layered data architecture that separates:

- Cloud-based raw data ingestion
- Bronze data storage
- Silver transformation and dimensional modeling
- Gold analytical aggregation
- Serving-layer consumption
- Governance and operational monitoring
- Development and production environments

The core architecture is:

```text
Azure ADLS Gen2
       |
       v
   Snowpipe
       |
       v
 Bronze Layer
 RAW_ORDERS
       |
       v
RAW_ORDERS_STREAM
       |
       v
SP_BRONZE_TO_SILVER()
       |
       +------------------+
       |                  |
       v                  v
 STG_ORDERS         DEAD_LETTER
       |
       v
STG_ORDERS_STREAM
       |
       v
SILVER_TO_STAR()
       |
       v
 Silver Star Schema
       |
       v
 Gold Dynamic Tables
       |
       v
 Serving Secure Views
       |
       v
 Streamlit / Analytics