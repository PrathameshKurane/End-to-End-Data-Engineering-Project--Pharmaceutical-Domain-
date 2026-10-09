# Pharma Data Analytics Platform

An end-to-end pharmaceutical data engineering project built using **Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, and Unity Catalog**. The platform follows the Medallion Architecture to ingest, transform, validate, and organize pharmaceutical data for business analytics.

## Project Overview

* **Architecture:** Medallion (Bronze → Silver → Gold)
* **Cloud:** Microsoft Azure
* **Data Processing:** PySpark and Spark SQL
* **Storage Format:** Delta Lake
* **Governance:** Databricks Unity Catalog
* **Source Format:** CSV
* **Data Domain:** Pharmaceutical and Healthcare Analytics

## Architecture

```text
     SOURCE SYSTEMS
  10 Pharmaceutical CSV Files
             |
             v
  AZURE DATA LAKE STORAGE
          Raw Layer
             |
             v
  +------------------------+
  |     BRONZE LAYER       |
  | Raw Delta Tables       |
  | Schema + Ingestion     |
  | Metadata               |
  +------------------------+
             |
             v
  +------------------------+
  |     SILVER LAYER       |
  | Clean & Validate       |
  | Deduplicate            |
  | Standardize & Enrich   |
  +------------------------+
             |
             v
  +------------------------+
  |      GOLD LAYER        |
  | Facts & Dimensions     |
  | Star Schema            |
  | Business Aggregations  |
  +------------------------+
             |
             v
    ANALYTICS & REPORTING
  Sales | Clinical | Inventory
  Prescriptions | Marketing
```

**Data flow:** CSV files → ADLS Gen2 → Bronze Delta tables → Silver Delta tables → Gold analytical tables.

Unity Catalog manages table permissions, data discovery, and supported lineage tracking across the platform.

## Source Data

The project processes 10 datasets covering pharmaceutical operations.

| Dataset                    |   Records |
| -------------------------- | --------: |
| Clinical trials            |       500 |
| Sales and revenue          |     1,000 |
| Inventory and supply chain |       800 |
| Prescriptions              |     1,500 |
| Side effects               |       600 |
| R&D pipeline               |       300 |
| Pricing and reimbursement  |       400 |
| Physician prescribing      |       700 |
| Marketing promotions       |       500 |
| Employees                  |       100 |
| **Total**                  | **6,400** |

*Record counts represent the supplied dataset and should be verified against actual ingestion results.*

## Medallion Architecture

### 1. Bronze — Raw Ingestion

**Objective:** Preserve source data and establish an auditable ingestion layer.

* Read CSV files using PySpark.
* Apply appropriate schemas where required.
* Add ingestion metadata:

  * `ingestion_timestamp`
  * `source_file`
  * `ingestion_batch_id`
* Write data as Delta tables registered in Unity Catalog.
* Retain source values and avoid business-level cleaning.

**Output:** Raw Bronze Delta tables.

> Note: Explicit type casting changes source representation. If exact source preservation is required, retain the original CSV files and ingest source columns without lossy conversions.

### 2. Silver — Cleansing and Transformation

**Objective:** Produce reliable, standardized, analytics-ready datasets.

| Dataset         | Key transformations                                                                      |
| --------------- | ---------------------------------------------------------------------------------------- |
| Clinical trials | Standardize trial phase and status; derive enrollment categories and completion flags    |
| Sales           | Validate revenue; categorize revenue and sales channels; derive year, month, and quarter |
| Inventory       | Calculate days to expiry, stock ratios, expiry risk, and stock status                    |
| Prescriptions   | Standardize frequency; categorize treatment duration and adherence                       |
| Side effects    | Standardize severity; derive severity scores, serious-event flags, and age groups        |
| R&D pipeline    | Standardize development stages; derive risk and project duration categories              |
| Pricing         | Standardize payer types; classify prices and patient assistance eligibility              |
| Physicians      | Derive experience levels, e-prescribing flags, and prescribing-volume categories         |
| Marketing       | Derive ROI and campaign performance categories                                           |
| Employees       | Apply appropriate data validation and standardization                                    |

Common processing steps include:

* Deduplication using business keys.
* Null and invalid-value handling.
* Data type validation.
* Standardization of categorical fields.
* Derived columns and business rules.
* Date-based attributes where applicable.

**Output:** Cleaned Silver Delta tables.

### 3. Gold — Analytics and Business Modeling

**Objective:** Organize curated data for efficient business reporting.

Proposed dimensional model:

**Dimensions**

* `dim_clinical_trials`
* `dim_drugs`
* `dim_physicians`
* `dim_date`

**Fact tables**

* `fact_sales`
* `fact_clinical_trials`


Gold tables combine validated business data, dimension keys, relevant measures, and aggregations.

Key implementation concepts:

* Star schema and fact-to-dimension relationships.
* Surrogate keys and business keys.
* Historical tracking with SCD Type 2 where required.
* Revenue, inventory, clinical, and campaign metrics.
* Aggregations for downstream reporting.

The final table design and record counts should be validated against the implemented transformations. Not every Silver record necessarily maps one-to-one to a Gold fact row.

## SCD Type 2 — Historical Tracking

SCD Type 2 preserves historical versions of dimension records rather than overwriting changes.

Typical columns:

| Column           | Purpose                                      |
| ---------------- | -------------------------------------------- |
| `trial_sk`       | Unique surrogate key for a dimension version |
| `trial_id`       | Stable business identifier                   |
| `effective_date` | Date the version becomes valid               |
| `end_date`       | Date the version expires                     |
| `is_current`     | Identifies the current version               |

**Example:** When a clinical trial's status changes, expire the existing version and insert a new version.

* Old record: `is_current = false`, with an appropriate `end_date`.
* New record: `is_current = true`, with a new `effective_date`.

Use a stable surrogate-key strategy and test reruns carefully to avoid duplicate historical versions.

## Technology Stack

| Technology                   | Purpose                                                |
| ---------------------------- | ------------------------------------------------------ |
| Azure Data Lake Storage Gen2 | Raw and curated data storage                           |
| Azure Databricks             | Distributed data processing                            |
| PySpark                      | Data ingestion and transformation                      |
| Spark SQL                    | SQL-based data processing                              |
| Delta Lake                   | ACID transactions, schema enforcement, and time travel |
| Unity Catalog                | Data governance, access control, and lineage           |
| Azure Synapse Analytics      | Optional downstream analytical consumption             |

**Storage format:** Delta Lake stores data in Parquet files with a transaction log.

## Performance and Data Quality

Potential optimization techniques, applied where appropriate to actual workload size:

* `OPTIMIZE` for Delta file compaction.
* Z-ORDER or liquid clustering where supported and beneficial.
* Partitioning only when data volume and query patterns justify it.
* Broadcast joins for suitably small dimension tables.
* Predicate and column pushdown.
* Pre-aggregated Gold tables for frequently used metrics.
* Data quality checks for nulls, duplicates, invalid ranges, and referential integrity.

`VACUUM` removes eligible obsolete data files according to retention settings. It should not be treated as a routine substitute for safe retention management.

## Business Use Cases

The platform supports analysis across eight major areas:

1. **Clinical trials:** Trial phase, status, efficacy, completion rates, sponsor performance, and adverse events.
2. **Sales and revenue:** Revenue trends, regional performance, sales channels, and top-performing drugs.
3. **Inventory:** Expiry risk, stock availability, reorder requirements, and supplier performance.
4. **Prescriptions:** Prescription frequency, refill patterns, adherence categories, and insurance analysis.
5. **Side effects:** Severity distribution, serious events, drug-level patterns, and demographic breakdowns.
6. **R&D pipeline:** Development stages, project duration, risk categories, and funding analysis.
7. **Physician prescribing:** Specialty trends, prescribing volumes, and e-prescribing adoption.
8. **Marketing:** Campaign ROI, budget utilization, engagement, reach, and conversion rates.

These are analytical use cases supported by the dataset, not claims of validated clinical outcomes.

## Repository Structure

```text
pharma-data-analytics-platform/
│
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_modeling.py
│
├── sql/
│   ├── create_catalog_and_schemas.sql
│   ├── gold_star_schema.sql
│   └── cleanup.sql
│
└── .gitignore
```

Adjust the structure to match the files actually included in the repository.

## Setup and Execution

1. Create or configure an Azure Data Lake Storage Gen2 account.
2. Configure Databricks access using a supported Unity Catalog storage credential and external location.
3. Create the catalog and Bronze, Silver, and Gold schemas.
4. Upload the source CSV files to the raw storage location.
5. Execute the Bronze ingestion notebook.
6. Run the Silver transformation notebook.
7. Build the Gold dimensions, facts, and business aggregates.
8. Validate record counts, data quality, keys, and analytical queries.

Use placeholders for storage paths, account names, and environment-specific settings. Never commit secrets, access keys, tokens, or credentials.

## Key Features
1.Medallion architecture (Bronze, Silver, Gold)
2.Data cleaning and quality checks
3.Delta tables for reliable data storage
4.Dimensional modeling for analytics
5.Data governance using Unity Catalog


## Future Enhancements

* Automate ingestion and transformations using Azure Data Factory or Databricks Workflows.
* Implement incremental processing and change data capture where supported by the source.
* Add automated data quality checks and pipeline monitoring.
* Build Power BI dashboards or Databricks SQL dashboards.
* Explore forecasting and anomaly detection using appropriately validated data.

## Key Takeaways

This project demonstrates a pharmaceutical data engineering workflow using **Azure, Databricks, PySpark, Delta Lake, and Unity Catalog**. It covers raw ingestion, data cleansing, business transformations, dimensional modeling, historical tracking, and analytical data preparation through a structured Medallion Architecture.

The repository should document implemented functionality separately from planned enhancements so that readers can clearly distinguish completed work from future scope.
