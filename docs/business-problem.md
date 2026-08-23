# FlowBridge Supply Chain Data Platform — Business Problem

## 1. Business Context

FlowBridge International Ltd. operates a global industrial process equipment distribution network that depends on timely supply-chain information from logistics partners.

The existing process relied on logistics partners exporting supply-chain data as JSON files and manually exchanging those files with the organization.

This created a significant gap between when operational events occurred and when the data became available for planning and analytics.

## 2. Problem

The existing data exchange process introduced several operational challenges:

- Manual JSON data exports and file exchanges
- Data arriving from multiple logistics partners
- Delayed data availability of approximately 7–10 days
- Stale inventory and shipment information
- Limited visibility into order fulfillment
- Difficulty monitoring supplier performance
- Delayed identification of shipment delays
- Increased manual effort for data preparation and reporting
- Lack of a centralized, governed analytics layer

The result was a supply-chain reporting process that was reactive rather than operationally responsive.

## 3. Engineering Requirements

The replacement platform needed to:

1. Automatically ingest incoming JSON files.
2. Support nested and semi-structured supply-chain data.
3. Eliminate polling-based ingestion where possible.
4. Process new data incrementally rather than repeatedly processing the entire dataset.
5. Separate raw, cleaned, and analytical data.
6. Validate and isolate rejected records.
7. Build a dimensional model for analytical workloads.
8. Support incremental updates to dimensions and facts.
9. Provide near-real-time analytical datasets.
10. Expose governed data to analysts and business users.
11. Provide operational monitoring and pipeline health alerts.
12. Support development and production environments.
13. Provide a controlled path for promoting the platform from DEV to PROD.
14. Support secure data sharing with external partners.

## 4. Proposed Solution

The solution is an event-driven Azure and Snowflake data platform.

Supply-chain JSON data is generated and/or received from external sources and landed in Azure Data Lake Storage Gen2.

Azure Event Grid and Storage Queue provide the event-driven notification mechanism used to trigger Snowpipe ingestion into Snowflake.

The Snowflake platform then processes the data through a medallion architecture:

```text
Azure ADLS Gen2
       │
       ▼
   Snowpipe
       │
       ▼
    BRONZE
       │
       │ Streams + Tasks
       ▼
 SILVER STAGE 1
       │
       │ Streams + Tasks
       ▼
 SILVER STAGE 2
       │
       ▼
   STAR SCHEMA
       │
       ▼
     GOLD
       │
       ▼
 SECURE VIEWS
       │
   ┌───┴────┐
   ▼        ▼
Streamlit  Data Sharing