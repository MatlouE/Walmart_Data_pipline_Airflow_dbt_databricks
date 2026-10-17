# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion with Auto Loader
# MAGIC
# MAGIC Reads the Parquet files that Airflow lands in `/Volumes/<catalog>/bronze/landing/<table>/`
# MAGIC and appends their rows to the Delta table `<catalog>.bronze.<table>`.
# MAGIC
# MAGIC * **Append-only, raw.** No deduplication here: if a row changed twice, bronze holds both versions.
# MAGIC   Picking the latest version per primary key is silver's job.
# MAGIC * **Each file is ingested once.** Auto Loader records processed files in the checkpoint, so re-running is safe.
# MAGIC * **`availableNow` trigger.** Processes everything new, then stops (batch-style, fits orchestration).
# MAGIC
# MAGIC Widget `table`: one table name, or `all` for every table.

# COMMAND ----------

from pyspark.sql import functions as F

TABLES = ["customers", "stores", "products", "employees", "orders", "order_items"]

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("table", "customers")  # a table name, or "all"

CATALOG = dbutils.widgets.get("catalog")
TABLE = dbutils.widgets.get("table")

if TABLE != "all" and TABLE not in TABLES:
    raise ValueError(f"unknown table: {TABLE}")

# Checkpoints live in their own volume, NOT inside the landing volume Auto Loader is watching.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")

# COMMAND ----------

def ingest(table: str) -> str:
    landing = f"/Volumes/{CATALOG}/bronze/landing/{table}"
    checkpoint = f"/Volumes/{CATALOG}/bronze/checkpoints/{table}"
    target = f"{CATALOG}.bronze.{table}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")  # where the inferred schema is stored
        .load(landing)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", f"{checkpoint}/stream")  # which files were already processed
        .option("mergeSchema", "true")  # allow new source columns to be added to the bronze table
        .trigger(availableNow=True)
        .toTable(target)  # creates the Delta table on first run
    )
    query.awaitTermination()
    return target

# COMMAND ----------

tables = TABLES if TABLE == "all" else [TABLE]

for t in tables:
    target = ingest(t)
    print(f"{target}: {spark.table(target).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC Run these for a table after ingesting it (change the name in the first line).

# COMMAND ----------

check_table = f"{CATALOG}.bronze.customers"

display(spark.sql(f"DESCRIBE TABLE {check_table}"))  # look at the column types Auto Loader inferred

display(spark.sql(f"""
    SELECT _source_file, count(*) AS rows
    FROM {check_table}
    GROUP BY _source_file
    ORDER BY _source_file
"""))  # one row per landed file: confirms each file was ingested once
