# Retail Intelligence Platform

An end-to-end AWS data platform that ingests Brazilian e-commerce data, builds a star schema data warehouse, and serves business analytics via a REST API — orchestrated by Apache Airflow and documented with dbt.

---

## Architecture

```
Olist Dataset (100k+ orders, 7 CSVs)
         ↓
Config-driven Python ingestion + Dead Letter Queue
         ↓
S3 Data Lake (raw → curated → star schema)
  - Hive partitioned: year/month
  - Parquet format
         ↓
AWS Glue + PySpark (3 jobs)
  - CSV to Parquet
  - Star schema builder
  - SCD Type 2 handler
         ↓
Redshift Serverless + dbt (19 tests passing)
  - raw_data → staging → marts
         ↓
Apache Airflow (3 DAGs)
  - daily_ingestion → transformation → dbt_transformation
         ↓
FastAPI (6 endpoints)
  - /revenue/monthly
  - /revenue/by-state
  - /products/top10
  - /customers/summary
  - /orders/late-delivery-rate
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Storage | AWS S3 (raw, curated, features, scripts, logs zones) |
| Processing | AWS Glue + PySpark |
| Catalog | AWS Glue Data Catalog |
| Query | Amazon Athena |
| Warehouse | Redshift Serverless (ap-southeast-2) |
| Transformation | dbt (staging + marts, 19 tests) |
| Orchestration | Apache Airflow 3.x |
| Serving | FastAPI + uvicorn |
| IaC | Terraform |
| Version Control | GitHub |

---

## Dataset

Brazilian E-Commerce Public Dataset by Olist (Kaggle)

| Table | Rows | Description |
|-------|------|-------------|
| orders | 99,441 | Order header records |
| order_items | 112,650 | Line items per order |
| customers | 99,441 | Customer details |
| products | 32,951 | Product catalogue |
| sellers | 3,095 | Seller details |
| payments | 103,886 | Payment records |
| reviews | 100,000 | Customer reviews |

---

## Star Schema

```
                    dim_date
                       |
dim_customer ─── fact_order_items ─── dim_product
                       |
                   dim_seller
```

**Fact table grain:** one row per order item

**Derived facts:**
- `total_amount` = price + freight_value
- `delivery_days` = delivered_date - order_date
- `is_late_delivery` = delivered > estimated (1/0)

---

## Key Design Decisions

**Config-driven ingestion** — adding a new data source requires only a YAML change, zero code changes.

**Dead Letter Queue** — failed uploads are logged to `failed_uploads.log` rather than silently lost, enabling replay.

**SCD Type 2** — customer dimension tracks historical city/plan changes with `valid_from`, `valid_to`, `is_current` — enabling point-in-time correct joins.

**Hive partitioning** — orders partitioned by `year/month` reduces Athena scan cost by ~95% for date-filtered queries.

**Redshift Serverless** — chosen over provisioned cluster to eliminate idle costs — only charges when queries run.

**dbt on Redshift** — SQL transformations are version controlled, tested, and documented. 19/19 tests passing.

---

## Airflow DAGs

| DAG | Schedule | Tasks |
|-----|----------|-------|
| `daily_ingestion` | 2am daily | run_ingestion → run_glue_crawler |
| `transformation` | 3am daily | csv_to_parquet → star_schema_builder |
| `dbt_transformation` | 4am daily | dbt_run → dbt_test |

---

## API Endpoints

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/revenue/monthly?year=2017` | Monthly revenue breakdown |
| GET | `/revenue/by-state` | Revenue by customer state |
| GET | `/products/top10` | Top 10 product categories |
| GET | `/customers/summary` | Customer summary by state |
| GET | `/orders/late-delivery-rate` | Late delivery rate by seller state |

**Sample response** (`/revenue/monthly?year=2017`):
```json
{
  "data": [
    {
      "year": 2017,
      "month": 11,
      "month_name": "November",
      "total_orders": 7451,
      "revenue": 1179143.77,
      "avg_order_value": 136.08
    }
  ]
}
```

---

## Project Structure

```
retail-intelligence-platform/
├── terraform/          # AWS infrastructure as code
│   ├── main.tf
│   ├── s3.tf
│   ├── glue.tf
│   └── redshift.tf
├── ingestion/          # Config-driven ingestion
│   ├── pipeline_config.yaml
│   └── ingest.py
├── glue_jobs/          # PySpark transformation jobs
│   ├── csv_to_parquet.py
│   └── star_schema_builder.py
├── dbt/                # dbt models
│   └── models/
│       ├── staging/
│       └── marts/
├── airflow/            # Airflow DAGs
│   └── dags/
│       ├── daily_ingestion_dag.py
│       ├── transformation_dag.py
│       └── dbt_dag.py
└── api/                # FastAPI serving layer
    └── main.py
```

---

## Setup

### Prerequisites
- AWS CLI configured (`ap-southeast-2`)
- Terraform installed
- Python 3.11+

### Deploy Infrastructure
```bash
cd terraform
terraform init
terraform apply
```

### Run Ingestion
```bash
cd ingestion
pip install -r requirements.txt
python ingest.py
```

### Run API Locally
```bash
cd api
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```


## What I'd Improve at Scale

- Replace local Airflow with AWS MWAA for production scheduling
- Add SageMaker Feature Store + XGBoost churn prediction model
- Implement Delta Lake / Apache Iceberg for ACID transactions
- Add Great Expectations for automated data quality monitoring
- Deploy FastAPI to AWS Lambda + API Gateway for serverless serving
- Add CI/CD via GitHub Actions for automated testing and deployment
- Add QuickSight dashboard for business stakeholders