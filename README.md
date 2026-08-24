# Insurance Data Engineering Pipeline

A scalable data engineering project implementing a Medallion
Architecture (Bronze → Silver → Gold) using PySpark, Databricks, and
Delta Lake.

## Project Overview

This project builds an end-to-end insurance data pipeline that
transforms raw policy, claims, and accident data into analytics-ready
business tables.

The pipeline includes:

-   Raw data ingestion
-   Data cleaning and standardization
-   Business-level aggregations
-   Preparation for BI analytics and dashboards

## Architecture

    Raw Data Sources
            |
            v
    Bronze Layer
    (Raw Ingestion)
            |
            v
    Silver Layer
    (Cleaning & Transformation)
            |
            v
    Gold Layer
    (Business Aggregations)
            |
            v
    Metabase (Future)

## Technologies Used

-   Python
-   PySpark
-   Apache Spark
-   Databricks
-   Delta Lake
-   SQL
-   GitHub
-   Metabase (planned)

## Dataset

The project uses insurance datasets:

-   Policies
-   Claims
-   Accident records

Raw datasets are not included in this repository due to size
limitations.

Dataset files should be placed under:

    data/raw/

Schema definitions are available under:

    schemas/

## Project Structure

    insurance-data-engineering/

    ├── README.md

    ├── notebooks/
    │   ├── 01_bronze_layer.ipynb
    │   ├── 02_silver_layer.ipynb
    │   └── 03_gold_layer.ipynb

    ├── schemas/
    │   ├── claims_schema.json
    │   ├── accidents_schema.json
    │   └── policies_schema.json

    ├── data/
    │   └── README.md

    ├── images/
    │   └── medallion_architecture.png

    └── requirements.txt

## Bronze Layer

The Bronze layer stores raw ingested data.

Operations:

-   Reading raw datasets
-   Preserving original structure
-   Adding ingestion timestamps
-   Creating Delta tables

Tables:

    bronze_claims
    bronze_accidents
    bronze_policies

## Silver Layer

The Silver layer performs data cleaning and standardization.

Operations:

-   Schema enforcement
-   Column renaming
-   Data type casting
-   Date parsing
-   Handling invalid values

Tables:

    silver_claims
    silver_accidents
    silver_policies

## Gold Layer

The Gold layer creates business-ready analytical tables.

### Claims

    gold_claims_daily
    gold_claims_weekly
    gold_claims_monthly

Includes:

-   Number of claims
-   Total claim amount
-   Rolling averages
-   Trend metrics

### Accidents

    gold_accidents_daily
    gold_accidents_weekly
    gold_accidents_monthly

Includes:

-   Accident counts
-   Accident trends
-   Vehicle involvement statistics
-   Geographic analysis

### Policies

    gold_policies_monthly

Includes:

-   Policies issued
-   Policies expired
-   Exposure metrics
-   Vehicle age analysis

## Data Quality

The pipeline applies data quality practices:

-   Schema validation
-   Data type consistency
-   Invalid date handling
-   Null investigation
-   Business rule checks

## Future Improvements

Planned improvements:

-   Connect Gold tables to Metabase
-   Build interactive BI dashboards
-   Create KPI monitoring views
-   Add automated data quality checks
-   Schedule Databricks workflows
-   Add CI/CD integration

## Metabase Dashboard (Future)

The Gold layer is designed for BI consumption.

Planned dashboards:

### Claims Dashboard

-   Claim trends
-   Claim amounts
-   Average claim cost

### Accident Dashboard

-   Accident frequency
-   High-risk locations
-   Accident trends

### Policy Dashboard

-   Policy growth
-   Exposure trends
-   Expiration analysis

## How to Run

1.  Clone the repository.

2.  Upload notebooks to Databricks.

3.  Configure dataset locations.

4.  Run notebooks in order:

```{=html}
<!-- -->
```
    01_bronze_layer
            |
            v
    02_silver_layer
            |
            v
    03_gold_layer

## Author

Yasna Kazemghamsari

Data Engineering \| PySpark \| Databricks \| Data Analytics
