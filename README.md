# Egypt Telecom Lakehouse: Streaming + Batch Medallion Pipeline

An end-to-end data engineering project that simulates an Egyptian telecom operator (Vodafone, Etisalat, Orange, WE) and builds a full lakehouse on top of it: Azure Event Hubs, Azure Data Factory, Databricks Lakeflow Declarative Pipelines (Bronze / Silver / Gold), SCD Type 2 customers, and a star/snowflake model ready for BI.

<img width="1837" height="795" alt="image" src="https://github.com/user-attachments/assets/cc8c2999-9188-4524-b1a1-b070f11a4611" />

## What it does

- Generates realistic telecom events (usage, recharges, wallet transactions, subscriptions, retail sales, B2B corporate orders) and streams them to Event Hubs.
- Loads the initial history (bulk events and 24 mapping tables) with Azure Data Factory into ADLS, then into Delta tables.
- Merges the bulk load and the live stream into one typed staging table.
- Cleans, validates and deduplicates events, then builds a customer dimension with SCD Type 2.
- Publishes a Gold layer: 26 dimensions and 4 fact tables.

## Architecture

<img width="1517" height="262" alt="image" src="https://github.com/user-attachments/assets/9a6b78d1-8e37-4183-8da1-4186e8ba0be5" />


## Repository layout

| File | Layer | Purpose |
|---|---|---|
| `generator/telecom_generator.py` | Source | Generates events, customer pool and customer history |
| `adf/` | Ingestion | ADF pipeline: Lookup (`files_array`) > ForEach > Copy to `raw/ingestion` |
| `notebooks/BronzeADLS.ipynb` | Bronze (batch) | Reads the JSON files and writes `bulk_telecom_events` and `map_*` as Delta |
| `pipeline/Ingest_Bronze.py` | Bronze (stream) | Reads Event Hubs through the Kafka endpoint into `telecom_events_raw` |
| `pipeline/Transform_Silver.py` | Silver | Staging, cleaning, SCD2, silver mapping tables, OBT |
| `pipeline/Model_Gold.sql` | Gold | Dimensions and fact tables |

## Layers

### Bronze
- **Streaming**: `telecom_events_raw` reads the `teletopic` Event Hub using the Kafka protocol (SASL_SSL). The connection string comes from a pipeline config value and is never hardcoded.
- **Batch**: an ADF pipeline copies the JSON files from GitHub to ADLS, and a notebook loads them into Delta tables in `telecom.onelakedatabase`.

### Silver
1. `stg_telecom_events`: two append flows (bulk and stream) into one table with a single schema.
2. `silver_telecom_events`: deduplication by `event_id` with a watermark on `ingested_at` (processing time, because the bulk file carries events up to 28 days old), string cleaning, Egyptian phone standardization (`+20...`), business-rule fixes, one comparable `event_amount_egp`, and data quality expectations (drop or warn).
3. `silver_customer_changes` > `silver_customer_scd2` > `silver_dim_customer`: SCD Type 2 using `create_auto_cdc_flow`, sequenced by event time, tracking plan, contract, device, loyalty tier, KYC status and governorate through an MD5 record hash.
4. `silver_map_*`: cleaned copy of each mapping table.
5. `silver_obt`: events enriched with all mappings (One Big Table). It is built for fast ad-hoc exploration and ML work (no joins needed), but Gold intentionally does not read from it: Gold is a star schema, so each dimension is defined once, SCD Type 2 stays correct, the four fact grains stay separate, and BI tools work best with it.

### Gold
- **Dimensions (26)**: `dim_date`, `dim_customer` (SCD2) and 24 dimensions as views over the silver mappings. Stores, governorates and industries are snowflaked.
- **Facts (4)**:
  - `fact_usage`: one call, data session, SMS, roaming or VAS record
  - `fact_transaction`: recharge, wallet transaction or subscription
  - `fact_sales`: one invoice line (SIM, device, number)
  - `fact_corporate_order`: one B2B order
- Facts join `dim_customer` on `event_timestamp BETWEEN scd_from_date AND scd_last_date`, so each event points to the customer version that was valid at that moment.

## Tech stack

Python, Faker, Azure Event Hubs, Azure Data Factory, ADLS Gen2, Databricks (Lakeflow Declarative Pipelines, Unity Catalog, Delta Lake), PySpark, Spark SQL.

## How to run

1. Create an Event Hubs namespace `TelecomEH` with a hub `teletopic`.
2. Create a `.env` file for the generator with your Event Hubs connection string, then run the generator to start streaming.
3. Put the mapping JSON files and the bulk file in `raw/ingestion`, then run the ADF pipeline.
4. Run `BronzeADLS.ipynb` to create the Delta tables.
5. Create a Databricks pipeline with the three pipeline files, and set the config value `connection_string` to your Event Hubs connection string.
6. Start the pipeline and query the Gold tables.

## Security note

Never commit connection strings, SAS tokens or `.env` files. Use Databricks secrets or pipeline configuration, and rotate any key that was ever pasted into a notebook.

## Possible next steps

- Auto Loader instead of the notebook for the batch load
- Orchestration with Databricks Jobs
- Power BI semantic model on top of Gold
- Churn prediction using `churn_risk_score` and usage facts
