# Spark Analytics and Machine Learning

This module runs Apache Spark jobs over the MongoDB-backed financial data warehouse.

## Setup

```powershell
cd C:\Users\adelg\Downloads\datawarehouse\datawarehouse\spark_analysis_ml
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Run Jobs

```powershell
.\run_spark.ps1 .\compute_yearly_summaries.py
.\run_spark.ps1 .\train_price_regression.py
```

## Outputs

The aggregation job writes to:

```text
analytics_yearly_summaries
```

The machine learning job writes to:

```text
analytics_price_predictions
```

Both collections are exposed by the Spring Boot API under:

```text
/api/v1/analytics
```
