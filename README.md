# Insurance Data Engineering Pipeline

An end-to-end insurance data engineering project implementing a Medallion Architecture (Bronze → Silver → Gold) using PySpark, Databricks, Delta Lake, SQL, ClickHouse, and Metabase.

The pipeline transforms raw insurance data into analytics-ready Gold tables and provides interactive BI dashboards.

---

## Architecture

```
Raw Data
    |
    v
Bronze Layer
    |
    v
Silver Layer
    |
    v
Gold Layer
    |
    v
ClickHouse
    |
    v
Metabase Dashboard
```

---

## Technologies

- Python
- PySpark
- Apache Spark
- Databricks
- Delta Lake
- SQL
- ClickHouse
- Metabase
- Docker
- GitHub

---

## Data Domains

The pipeline processes three main insurance domains:

- Claims
- Accidents
- Policies

---

## Medallion Layers

### Bronze Layer

Raw ingestion layer that stores source data with minimal transformation.

Tables:

```
bronze_claims
bronze_accidents
bronze_policies
```

---

### Silver Layer

Cleaning and transformation layer.

Operations:

- Schema validation
- Data type standardization
- Data cleaning
- Date parsing

Tables:

```
silver_claims
silver_accidents
silver_policies
```

---

### Gold Layer

Business-ready analytical tables designed for BI reporting.

### Claims

```
gold_claims_daily
gold_claims_weekly
gold_claims_monthly
```

Metrics:

- Number of claims
- Total claim amount
- Claim trends
- Rolling averages
- Growth metrics


### Accidents

```
gold_accidents_daily
gold_accidents_weekly
gold_accidents_monthly
```

Metrics:

- Accident frequency
- Accident trends
- Time-based analysis
- Geographic insights


### Policies

```
gold_policies_monthly
```

Metrics:

- Policies issued
- Policies expired
- Exposure metrics
- Vehicle age analysis

---

# Metabase Dashboard

The Gold layer is connected to Metabase for interactive analytics and visualization.

Dashboard sections:

## Executive Overview

- Total claims
- Total claim amount
- Total accidents
- Total policies issued
- Overall trends


## Claims Analysis

- Monthly claim trends
- Claim amount analysis
- Claim amount breakdown by type
- Claim severity analysis


## Accident Analysis

- Monthly and weekly accident trends
- Accident patterns
- Location analysis


## Policy Analysis

- Policy growth trends
- Exposure trends
- Vehicle age analysis
- Expiration trends


Dashboard screenshots are available in:

```
images/
```

### Executive Overview

![Executive Overview](images/executive_overview.png)

### Claims Analysis

![Claims Analysis](images/claims_analysis.png)

### Accident Analysis

![Accident Analysis](images/accident_analysis.png)

### Policy Analysis

![Policy Analysis](images/policy_analysis.png)

---

## Project Structure

```
insurance-data-engineering/

├── notebooks/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── data/
│
├── setup/
│
├── images/
│   ├── executive_overview.png
│   ├── claims_analysis.png
│   ├── accident_analysis.png
│   └── policy_analysis.png
│
├── requirements.txt
└── README.md
```

---

## How to Run

1. Clone the repository.

2. Upload notebooks to Databricks.

3. Run the pipeline in order:

```
Bronze Layer
      |
      v
Silver Layer
      |
      v
Gold Layer
```

4. Load Gold tables into ClickHouse.

5. Connect ClickHouse to Metabase for dashboard visualization.

---

## Author

Yasna Kazemghamsari

Data Engineering | PySpark | Databricks | Data Analytics