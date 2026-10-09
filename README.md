# End-to-End-Data-Engineering-Project--Pharmaceutical-Domain-

Pharma Data Analytics Platform - Architecture Deep Dive
Complete Architecture Overview
This project implements an end-to-end Pharmaceutical Data Analytics Platform using the Medallion Architecture (Bronze → Silver → Gold) with Delta Lake on Databricks Unity Catalog.

Architecture Layers
1. Raw Data Layer (Source Systems)

┌─────────────────────────────────────────────────────────────────────────────┐
│                         SOURCE DATA LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │  Clinical    │ │    Sales     │ │  Inventory   │ │ Prescriptions│        │
│  │  Trials      │ │   Revenue    │ │ Supply Chain │ │              │        │
│  │  (500 recs)  │ │  (1000 recs) │ │  (800 recs)  │ │ (1500 recs)  │        │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
│                                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │   Side       │ │   R&D        │ │   Pricing    │ │  Physician   │        │
│  │   Effects    │ │   Pipeline   │ │ Reimbursement│ │  Prescribing │        │
│  │  (600 recs)  │ │  (300 recs)  │ │  (400 recs)  │ │  (700 recs)  │        │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
│                                                                             │
│  ┌──────────────┐                                                           │
│  │  Marketing   │                                                           │
│  │  Promotions  │                                                           │
│  │  (500 recs)  │                                                           │
│  └──────────────┘                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
2. Bronze Layer (Raw Ingestion)

┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                                        │
│                    (Immutable Raw Data)                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Purpose:                                                                   │
│  • Land raw data exactly as received from source systems                    │
│  • Preserve original data with no transformations                           │
│  • Provide audit trail and data lineage                                     │
│                                                                             │
│  Transformations Applied:                                                   │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │ 1. Schema Enforcement - Applied explicit schemas to all CSV files  │     │
│  │ 2. Data Type Casting - Converted strings to proper data types      │     │
│  │ 3. Metadata Addition - Added tracking columns:                     │     │
│  │    • ingestion_timestamp                                           │     │
│  │    • source_file                                                   │     │
│  │    • ingestion_batch_id                                            │     │
│  │ 4. No Data Cleaning - Raw data preserved as-is                     │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
│  Storage: Delta Tables in ADLS Gen2 (bronze container)                      │
│  Format: Delta Lake with ACID transactions                                  │
│                                                                             │
│  Tables Created:                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │ bronze_clinical_trials  │ bronze_sales           │                 │     │
│  │ bronze_inventory        │ bronze_prescriptions   │                 │     │
│  │ bronze_side_effects     │ bronze_rd_pipeline     │                 │     │
│  │ bronze_pricing          │ bronze_physician       │                 │     │
│  │ bronze_marketing        │ bronze_employees       │                 │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
3. Silver Layer (Cleaned & Enriched)

┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                                        │
│                    (Cleaned & Enriched Data)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Purpose:                                                                   │
│  • Clean, validate, and standardize data                                    │
│  • Enrich with derived columns and business logic                           │
│  • Remove duplicates and handle NULL values                                 │
│                                                                             │
│  Transformations Applied by Table:                                          │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │ CLINICAL TRIALS:                                                   │     │
│  │ • Standardized trial_phase (Phase I-IV)                            │     │
│  │ • Standardized trial_status (Active, Completed, etc.)              │     │
│  │ • Added enrollment_category (Small/Medium/Large)                   │     │
│  │ • Added is_completed, is_ongoing flags                             │     │
│  │ • Extracted trial_year, trial_month                                │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ SALES:                                                             │     │
│  │ • Standardized sales_channel (Hospital/Retail/Online/Wholesale)    │     │
│  │ • Added is_high_value flag (>$1M revenue)                          │     │
│  │ • Added revenue_category (Low/Medium/High/Very High)               │     │
│  │ • Extracted sales_year, sales_month, sales_quarter                 │     │
│  │ • Removed invalid records (total_revenue_usd <= 0)                 │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ INVENTORY:                                                         │     │
│  │ • Calculated days_to_expiry                                        │     │
│  │ • Added expiry_risk (Critical/High/Medium/Low)                     │     │
│  │ • Calculated stock_ratio (quantity/reorder_level)                  │     │
│  │ • Added stock_status (Critical/Below/Optimal/Overstocked)          │     │
│  │ • Standardized drug_type                                           │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ PRESCRIPTIONS:                                                     │     │
│  │ • Standardized frequency (Once Daily/Twice Daily/Weekly/etc.)      │     │
│  │ • Added duration_category (Short/Medium/Long-term)                 │     │
│  │ • Added adherence_level (High/Medium/Low)                          │     │
│  │ • Extracted prescription_year, month, quarter                      │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ SIDE EFFECTS:                                                      │     │
│  │ • Standardized severity (Mild/Moderate/Severe/Life-threatening)    │     │
│  │ • Added severity_score (1-4)                                       │     │
│  │ • Added is_serious flag (severity_score >= 3)                      │     │
│  │ • Added age_group (Pediatric/Young Adult/Adult/Geriatric)          │     │
│  │ • Added is_fatal flag                                              │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ R&D PIPELINE:                                                      │     │
│  │ • Standardized development_stage                                   │     │
│  │ • Added risk_level (1-3)                                           │     │
│  │ • Added is_high_risk flag                                          │     │
│  │ • Calculated project_duration_days                                 │     │
│  │ • Added duration_category (Short/Medium/Long)                      │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ PRICING:                                                           │     │
│  │ • Standardized payer_type                                          │     │
│  │ • Added generic_available_flag                                     │     │
│  │ • Added patient_assistance_flag                                    │     │
│  │ • Added price_category (Low/Medium/High/Very High)                 │     │
│  │ • Added price_increase_category                                    │     │
│  ├────────────────────────────────────────────────────────────────────┤     │
│  │ PHYSICIAN PRESCRIBING:                                             │     │
│  │ • Added experience_level (Junior/Mid/Senior/Expert)                │     │
│  │ • Added e_prescribing_flag                                         │     │
│  │ • Added sample_drugs_flag                                          │     │
│  │ • Added prescription_volume_category                               │     │
│  │ • Added satisfaction_category (Excellent/Good/Average/Poor)        │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
│  Storage: Delta Tables in ADLS Gen2 (silver container)                      │
└─────────────────────────────────────────────────────────────────────────────┘
4. Gold Layer (Star Schema & Analytics)
text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                          │
│                    (Star Schema & Business Insights)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    DIMENSION TABLES                                  │   │
│  ├──────────────────────────────────────────────────────────────────────┤   │
│  │                                                                      │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                    │   │
│  │  │  dim_clinical_trials │  │     dim_drugs       │                   │   │
│  │  ├─────────────────────┤  ├─────────────────────┤                    │   │
│  │  │ trial_sk (PK)       │  │ drug_sk (PK)        │                    │   │
│  │  │ trial_id            │  │ drug_name           │                    │   │
│  │  │ drug_name           │  │ effective_date      │                    │   │
│  │  │ condition_treated   │  │ is_current          │                    │   │
│  │  │ trial_phase         │  └─────────────────────┘                    │   │
│  │  │ trial_status        │                                             │   │
│  │  │ sponsor             │  ┌─────────────────────┐                    │   │
│  │  │ patient_count       │  │  dim_physicians     │                    │   │
│  │  │ enrollment_category │  ├─────────────────────┤                    │   │
│  │  │ is_completed        │  │ physician_sk (PK)   │                    │   │
│  │  │ is_ongoing          │  │ physician_id       │                     │   │
│  │  │ trial_year          │  │ physician_name     │                     │   │
│  │  │ trial_month         │  │ specialty          │                     │   │
│  │  └─────────────────────┘  │ years_experience   │                     │   │
│  │                           │ experience_level   │                     │   │
│  │  ┌─────────────────────┐  │ hospital_affiliation│                    │   │
│  │  │     dim_date        │  │ practice_type      │                     │   │
│  │  ├─────────────────────┤  │ state              │                     │   │
│  │  │ date_sk (PK)        │  │ prescription_volume│                     │   │
│  │  │ date                │  │ most_prescribed    │                     │   │
│  │  │ year, quarter       │  │ patient_satisfaction│                    │   │
│  │  │ month, day          │  └─────────────────────┘                    │   │
│  │  │ day_of_week         │                                             │   │
│  │  │ day_name            │                                             │   │
│  │  │ month_name          │                                             │   │
│  │  │ weekday_indicator   │                                             │   │
│  │  └─────────────────────┘                                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     FACT TABLES                                      │   │
│  ├──────────────────────────────────────────────────────────────────────┤   │
│  │                                                                      │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                    │   │
│  │  │    fact_sales       │  │ fact_clinical_trials│                    │   │
│  │  ├─────────────────────┤  ├─────────────────────┤                    │   │
│  │  │ sale_id (PK)        │  │ trial_id (PK)       │                    │   │
│  │  │ drug_sk (FK)        │  │ trial_sk (FK)       │                    │   │
│  │  │ date_sk (FK)        │  │ date_sk (FK)        │                    │   │
│  │  │ sales_channel       │  │ drug_name           │                    │   │
│  │  │ units_sold          │  │ condition_treated   │                    │   │
│  │  │ unit_price_usd      │  │ patient_count       │                    │   │
│  │  │ total_revenue_usd   │  │ efficacy_rate       │                    │   │
│  │  │ region, country     │  │ placebo_efficacy    │                    │   │
│  │  │ sales_rep_id        │  │ adverse_events_count│                    │   │
│  │  │ sales_year/month/qtr│  │ serious_adverse     │                    │   │
│  │  │ is_high_value       │  │ budget_millions     │                    │   │
│  │  │ revenue_category    │  │ completion_rate     │                    │   │
│  │  └─────────────────────┘  └─────────────────────┘                    │   │
│  │                                                                      │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                    │   │
│  │  │ fact_prescriptions  │  │   fact_inventory    │                    │   │
│  │  ├─────────────────────┤  ├─────────────────────┤                    │   │
│  │  │ prescription_id(PK) │  │ inventory_id (PK)   │                    │   │
│  │  │ physician_sk (FK)   │  │ drug_name           │                    │   │
│  │  │ date_sk (FK)        │  │ batch_number        │                    │   │
│  │  │ patient_id          │  │ warehouse_location  │                    │   │
│  │  │ drug_name           │  │ drug_type           │                    │   │
│  │  │ dosage_mg           │  │ quantity_in_stock   │                    │   │
│  │  │ frequency           │  │ reorder_level       │                    │   │
│  │  │ duration_days       │  │ safety_stock        │                    │   │
│  │  │ refill_count        │  │ supplier            │                    │   │
│  │  │ adherence_score     │  │ lead_time_days      │                    │   │
│  │  │ adherence_level     │  │ expiry_date         │                    │   │
│  │  │ copay_amount        │  │ batch_quality_score │                    │   │
│  │  │ insurance_provider  │  │ days_to_expiry      │                    │   │
│  │  └─────────────────────┘  │ expiry_risk         │                    │   │
│  │                           │ stock_status        │                    │   │
│  │  ┌─────────────────────┐  └─────────────────────┘                    │   │
│  │  │  fact_marketing     │                                             │   │
│  │  ├─────────────────────┤                                             │   │
│  │  │ campaign_id (PK)    │                                             │   │
│  │  │ date_sk (FK)        │                                             │   │
│  │  │ drug_name           │                                             │   │
│  │  │ campaign_name       │                                             │   │
│  │  │ channel             │                                             │   │
│  │  │ target_audience     │                                             │   │
│  │  │ budget_usd          │                                             │   │
│  │  │ spent_usd           │                                             │   │
│  │  │ impressions         │                                             │   │
│  │  │ clicks              │                                             │   │
│  │  │ conversion_rate     │                                             │   │
│  │  │ reach_count         │                                             │   │
│  │  │ engagement_rate     │                                             │   │
│  │  │ roi_percent         │                                             │   │
│  │  │ roi_category        │                                             │   │
│  │  │ drug_type           │                                             │   │
│  │  │ seasonal            │                                             │   │
│  │  └─────────────────────┘                                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘

Data Flow Architecture
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW DIAGRAM                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐       │
│  │   RAW CSV   │───▶│   BRONZE    │───▶│   SILVER   │───▶ │    GOLD    │       │
│  │   FILES     │    │   LAYER     │    │   LAYER     │    │   LAYER     │       │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘       │
│        │                  │                  │                  │               │
│        ▼                  ▼                  ▼                  ▼               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐       │
│  │  10 CSV     │    │  Delta      │    │  Delta      │    │  Delta      │       │
│  │  Files      │    │  Tables     │    │  Tables     │    │  Tables     │       │
│  │  6,400 recs │    │  6,400 recs │    │  6,400 recs │    │  5,257 recs │       │  
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘       │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                         TRANSFORMATIONS                                  │   │
│  ├──────────────────────────────────────────────────────────────────────────┤   │
│  │                                                                          │   │
│  │  RAW → BRONZE:                                                           │   │
│  │  • Schema enforcement                                                    │   │
│  │  • Metadata addition (timestamp, source, batch_id)                       │   │
│  │  • No data cleaning                                                      │   │
│  │                                                                          │   │
│  │  BRONZE → SILVER:                                                        │   │
│  │  • Deduplication (dropDuplicates on primary keys)                        │   │
│  │  • NULL handling (coalesce)                                              │   │
│  │  • Data standardization (standardize categorical values)                 │   │
│  │  • Derived columns (age groups, categories, flags)                       │   │
│  │  • Date extraction (year, month, quarter)                                │   │
│  │  • Business logic (expiry_risk, stock_status, adherence_level)           │   │
│  │                                                                          │   │
│  │  SILVER → GOLD:                                                          │   │
│  │  • Star schema creation (4 dimensions, 5 facts)                          │   │
│  │  • Surrogate key generation (monotonically_increasing_id)                │   │
│  │  • SCD Type 2 implementation (effective_date, is_current)                │   │
│  │  • Aggregations (SUM, AVG, COUNT, etc.)                                  │   │
│  │  • Business calculations (ROI, revenue categories, etc.)                 │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘

 Technology Stack
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TECHNOLOGY STACK                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        CLOUD INFRASTRUCTURE                            │     │
│  ├────────────────────────────────────────────────────────────────────────┤     │
│  │  Azure Portal                                                          │     │
│  │  ├── Resource Group: rg-pharma-analytics-prod                          │     │
│  │  ├── Storage Account: pharmadatalake789 (ADLS Gen2)                    │     │
│  │  │   ├── raw container (CSV source files)                              │     │
│  │  │   ├── bronze container (Delta tables)                               │     │
│  │  │   ├── silver container (Delta tables)                               │     │
│  │  │   └── gold container (Delta tables)                                 │     │
│  │  ├── Databricks Workspace: db-pharma-analytics-prod                    │     │
│  │  │   └── Unity Catalog Enabled (Premium Tier)                          │     │
│  │  └── Synapse Workspace: synapse-pharma-prod                            │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        DATA ENGINEERING                                │     │
│  ├────────────────────────────────────────────────────────────────────────┤     │
│  │  Databricks                                                            │     │
│  │  ├── Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1)                       │     │
│  │  ├── Unity Catalog (Data Governance)                                   │     │
│  │  ├── Delta Lake (ACID Transactions)                                    │     │
│  │  ├── PySpark (Data Processing)                                         │     │
│  │  └── SQL (Business Queries)                                            │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        DATA FORMATS                                    │     │
│  ├────────────────────────────────────────────────────────────────────────┤     │
│  │  Source: CSV (10 files)                                                │     │
│  │  Storage: Delta Lake (Parquet + Transaction Log)                       │     │
│  │  Query: Spark SQL / PySpark DataFrames                                 │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────┘

 Data Volume & Statistics

┌─────────────────────────────────────────────────────────────────────────────────┐
│                       DATA VOLUME STATISTICS                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                         RECORD COUNTS                                 │      │
│  ├──────────────────┬──────────┬──────────┬──────────┬───────────────────┤      │
│  │ Table            │ Raw      │ Bronze   │ Silver   │ Gold              │      │
│  ├──────────────────┼──────────┼──────────┼──────────┼───────────────────┤      │
│  │ Clinical Trials  │ 500      │ 500      │ 500      │ 500 (Fact)        │      │
│  │ Sales            │ 1,000    │ 1,000    │ 1,000    │ 1,000 (Fact)      │      │
│  │ Inventory        │ 800      │ 800      │ 800      │ 800 (Fact)        │      │
│  │ Prescriptions    │ 1,500    │ 1,500    │ 1,500    │ 1,500 (Fact)      │      │
│  │ Side Effects     │ 600      │ 600      │ 600      │ -                 │      │
│  │ R&D Pipeline     │ 300      │ 300      │ 300      │ -                 │      │
│  │ Pricing          │ 400      │ 400      │ 400      │ -                 │      │
│  │ Physician        │ 700      │ 700      │ 700      │ 700 (Dim)         │      │
│  │ Marketing        │ 500      │ 500      │ 500      │ 500 (Fact)        │      │
│  │ Employees        │ 100      │ 100      │ 100      │ -                 │      │
│  ├──────────────────┼──────────┼──────────┼──────────┼───────────────────┤      │
│  │ TOTAL            │ 6,400    │ 6,400    │ 6,400    │ 5,257             │      │
│  └──────────────────┴──────────┴──────────┴──────────┴───────────────────┘      │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                         STORAGE LOCATIONS                              │     │
│  ├────────────────────────────────────────────────────────────────────────┤     │
│  │  Raw:     abfss://raw@adl456s.dfs.core.windows.net/                    │     │
│  │  Bronze:  abfss://bronze@adl456s.dfs.core.windows.net/                 │     │
│  │  Silver:  abfss://silver@adl456s.dfs.core.windows.net/                 │     │
│  │  Gold:    abfss://gold@adl456s.dfs.core.windows.net/                   │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────┘

Business Problems Solved (50+)

┌─────────────────────────────────────────────────────────────────────────────────┐
│                   BUSINESS PROBLEMS SOLVED                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │ 1. CLINICAL TRIALS ANALYSIS (15 Problems)                              │     │
│  ├────────────────────────────────────────────────────────────────────────┤     │
│  │ • Trial status distribution                                            │     │
│  │ • Trial phase distribution                                             │     │
│  │ • Top drugs by efficacy                                                │     │
│  │ • Trials by sponsor                                                    │     │
│  │ • Adverse events by phase                                              │     │
│  │ • High efficacy, low adverse events                                    │    │
│  │ • Budget vs efficacy analysis                                          │    │
│  │ • Efficacy by enrollment category                                      │    │
│  │ • Publication status distribution                                      │    │
│  │ • Sponsor success rate                                                 │    │
│  │ • Efficacy vs placebo comparison                                       │    │
│  │ • Data quality by phase                                                │    │
│  │ • Serious adverse events analysis                                      │    │
│  │ • Trial duration analysis                                              │    │
│  │ • Completion rate by sponsor                                           │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 2. SALES & REVENUE ANALYSIS (10 Problems)                              │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Sales by region                                                      │    │
│  │ • Sales by channel                                                     │    │
│  │ • Top drugs by revenue                                                 │    │
│  │ • Monthly revenue trend                                                │    │
│  │ • High value sales analysis                                            │    │
│  │ • Seasonal sales patterns                                              │    │
│  │ • Regional revenue contribution                                        │    │
│  │ • Channel performance                                                  │    │
│  │ • Unit price analysis                                                  │    │
│  │ • Sales rep performance                                                │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 3. INVENTORY ANALYSIS (10 Problems)                                    │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Stock status distribution                                            │    │
│  │ • Expiry risk analysis                                                 │    │
│  │ • Warehouse inventory summary                                          │    │
│  │ • Critical stock items                                                 │    │
│  │ • Supplier performance                                                 │    │
│  │ • Batch quality analysis                                               │    │
│  │ • Lead time analysis                                                   │    │
│  │ • Storage cost analysis                                                │    │
│  │ • Reorder efficiency                                                   │    │
│  │ • Drug type distribution                                               │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 4. PRESCRIPTION ANALYSIS (10 Problems)                                 │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Frequency distribution                                               │    │
│  │ • Adherence level distribution                                         │    │
│  │ • Most prescribed drugs                                                │    │
│  │ • Insurance provider analysis                                          │    │
│  │ • Duration category distribution                                       │    │
│  │ • Copay analysis                                                       │    │
│  │ • Refill patterns                                                      │    │
│  │ • Patient demographics                                                 │    │
│  │ • Specialty prescribing patterns                                       │    │
│  │ • Adherence vs duration                                                │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 5. SIDE EFFECTS ANALYSIS (10 Problems)                                 │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Severity distribution                                                │    │
│  │ • Most common side effects                                             │    │
│  │ • Side effects by drug                                                 │    │
│  │ • Age group analysis                                                   │    │
│  │ • Reporter type distribution                                           │    │
│  │ • Fatal events analysis                                                │    │
│  │ • Serious events by drug                                               │    │
│  │ • Seasonal pattern analysis                                            │    │
│  │ • Country-wise distribution                                            │    │
│  │ • Concomitant medications analysis                                     │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 6. R&D PIPELINE ANALYSIS (10 Problems)                                 │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Development stage distribution                                       │    │
│  │ • Risk assessment distribution                                         │    │
│  │ • Funding source analysis                                              │    │
│  │ • Disease area focus                                                   │    │
│  │ • Priority level distribution                                          │    │
│  │ • Patent analysis                                                      │    │
│  │ • Project cost analysis                                                │    │
│  │ • Team size analysis                                                   │    │
│  │ • Collaboration partner analysis                                       │    │
│  │ • Timeline analysis                                                    │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 7. PHYSICIAN PRESCRIBING ANALYSIS (10 Problems)                        │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • Experience level distribution                                        │    │
│  │ • Specialty distribution                                               │    │
│  │ • Prescription volume by practice type                                 │    │
│  │ • Top prescribing physicians                                           │    │
│  │ • Satisfaction vs prescription volume                                  │    │
│  │ • E-prescribing adoption                                               │    │
│  │ • Off-label prescribing analysis                                       │    │
│  │ • Sample drug acceptance                                               │    │
│  │ • Pharma visits analysis                                               │    │
│  │ • Patient satisfaction drivers                                         │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ 8. MARKETING ANALYSIS (10 Problems)                                    │    │
│  ├────────────────────────────────────────────────────────────────────────┤    │
│  │ • ROI category distribution                                            │    │
│  │ • Channel performance                                                  │    │
│  │ • Target audience analysis                                             │    │
│  │ • Seasonal campaign performance                                        │    │
│  │ • Drug type campaign performance                                       │    │
│  │ • Budget utilization analysis                                          │    │
│  │ • Conversion rate analysis                                             │    │
│  │ • Engagement rate analysis                                             │    │
│  │ • Reach analysis                                                       │    │
│  │ • Campaign duration analysis                                           │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘

Key Architecture Insights
1. Why Medallion Architecture?
Layer	Purpose	Benefits
Bronze	Raw data landing	Audit trail, data lineage, reprocessing capability
Silver	Cleaned data	Single source of truth, consistent data quality
Gold	Business analytics	Optimized for queries, star schema, KPIs ready

2. Why Delta Lake?
Feature	Benefit
ACID Transactions	Data consistency, no corruption
Time Travel	View historical data, rollback changes
Schema Evolution	Handle schema changes gracefully
Optimize & Vacuum	Performance optimization, storage cleanup
Unified Batch/Streaming	Single engine for all workloads

3. Why Unity Catalog?
Feature	Benefit
Centralized Governance	Single place for permissions
Data Lineage	Track data flow across layers
Schema Registry	Consistent schema management
Data Discovery	Easy to find and use data

4. Star Schema Benefits
Feature	Benefit
Query Performance	Simple joins between fact and dimension tables, predictable query patterns, and optimized aggregations
Business User Friendly	Provides an intuitive and easy-to-understand data model while reducing complexity
Scalability	Allows new dimensions and facts to be added easily without breaking the existing model
Consistency	Creates a single source of truth for dimensions, ensures consistent definitions, and reduces data redundancy

 SCD Type 2 Implementation
Feature / Component	Purpose / Benefit
Surrogate Key (trial_sk)	Provides a unique identifier for each historical version of a dimension record
Business Key (trial_id)	Identifies the real-world clinical trial and remains consistent across record changes
Effective Date	Captures when a particular version of the record became active
End Date	Captures when the record version stopped being active due to a change
Current Flag (is_current)	Identifies the latest active version of the record
Historical Tracking	Preserves previous versions of records instead of overwriting them
Change Handling	When a trial status changes, the existing record is closed and a new version is inserted
Example	Old record → is_current = False, end_date = change_date; New record → is_current = True, effective_date = change_date


Performance Optimizations
Optimization Area	Benefit
Delta Lake Optimizations	Improves query performance using Z-ORDER, data skipping, Bloom filters, file compaction with OPTIMIZE, and storage cleanup with VACUUM
Clustering & Partitioning	Organizes data by columns such as trial_year, sales_year, expiry_risk, and prescription_year to reduce data scanning and improve query performance
Query Optimizations	Improves query efficiency using star schema joins, pre-aggregated Gold layer data, Delta caching, and broadcast joins for small dimension tables

Next Steps & Enhancements
Enhancement Area	Purpose / Benefit
Real-Time Data Ingestion	Enables real-time pharma data processing using Azure Event Hubs, Delta Live Tables (DLT), and Structured Streaming
Advanced Analytics & ML	Supports drug efficacy prediction, patient adherence forecasting, sales forecasting, side-effect pattern detection, and inventory demand forecasting
Visualization & Reporting	Provides business insights through Power BI dashboards, Azure Synapse reports, Databricks SQL dashboards, and real-time monitoring
Data Governance	Strengthens data management through data quality rules, data lineage tracking, PII/PHI masking, and audit logging

Key Takeaways
Aspect	Key Insight
Architecture	Medallion architecture (Bronze→Silver→Gold) provides clear data progression
Storage	Delta Lake provides ACID transactions, time travel, and schema evolution
Governance	Unity Catalog enables centralized data governance and lineage
Schema	Star schema (Dimensions + Facts) optimizes for analytical queries
Transformations	Silver layer handles cleaning, Gold layer handles business logic
Scalability	All layers can be scaled independently
Cost	Storage tiering (Bronze/Silver/Gold) optimizes costs


Delete all created Resources, credential, external location, catalog, schema, delta table
%sql
-- Drop all bronze tables
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_clinical_trials;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_sales;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_inventory;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_prescriptions;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_side_effects;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_rd_pipeline;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_pricing;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_physician;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_marketing;
DROP TABLE IF EXISTS pharma_catalog_prod.bronze.bronze_employees;

%sql
-- Drop all silver tables
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_clinical_trials;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_sales;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_inventory;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_prescriptions;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_side_effects;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_rd_pipeline;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_pricing;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_physician;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_marketing;
DROP TABLE IF EXISTS pharma_catalog_prod.silver.silver_employees;

%sql
-- Drop fact tables
DROP TABLE IF EXISTS pharma_catalog_prod.gold.fact_sales;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.fact_clinical_trials;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.fact_prescriptions;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.fact_inventory;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.fact_marketing;

-- Drop dimension tables
DROP TABLE IF EXISTS pharma_catalog_prod.gold.dim_clinical_trials;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.dim_drugs;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.dim_physicians;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.dim_date;

-- Drop any other gold tables if they exist
DROP TABLE IF EXISTS pharma_catalog_prod.gold.sales_aggregates;
DROP TABLE IF EXISTS pharma_catalog_prod.gold.drug_performance_summary;

%sql
-- This will drop the catalog and all its contents
DROP CATALOG IF EXISTS pharma_catalog_prod CASCADE;


%sql
-- Drop external locations
DROP EXTERNAL LOCATION IF EXISTS raw_location_prod;
DROP EXTERNAL LOCATION IF EXISTS bronze_location_prod;
DROP EXTERNAL LOCATION IF EXISTS silver_location_prod;
DROP EXTERNAL LOCATION IF EXISTS gold_location_prod;


