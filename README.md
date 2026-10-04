# Walmart End-to-End Data Engineering Pipeline

An enterprise-grade Lakehouse data pipeline built using Apache Airflow, dbt, Databricks (Delta Lake), and Docker. This project simulates an end-to-end modern data engineering workflow by ingesting relational and semi-structured operational data, processing it through a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), and delivering an optimized One Big Table (OBT) and Star Schema for downstream analytics.

## Local environment

This project is pinned to Python 3.10. The container configuration and Airflow 2.8.1 dependency set are built for Python 3.10, so newer interpreters such as Python 3.14 can fail during package installation.

Use a Python 3.10 environment before installing dependencies:

```bash
uv venv --python 3.10 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```
