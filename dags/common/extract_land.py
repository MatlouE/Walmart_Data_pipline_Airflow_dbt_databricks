"""Extract changed rows from the source Postgres and land them as Parquet in a UC Volume.

Flow per table:
  1. read the stored watermark (max updated_timestamp already landed)
  2. SELECT rows with updated_timestamp > watermark
  3. write them to one Parquet file and upload to the landing Volume
  4. ONLY THEN advance the watermark

Step 4 happening last makes the task safe to retry: if it dies after the upload
but before the watermark moves, the retry re-extracts the same rows and
overwrites the same file (the file name comes from the run id, not the clock).

Run standalone from the codespace (host side of the Docker port mapping):
    SOURCE_PG_HOST=localhost SOURCE_PG_PORT=5433 python dags/common/extract_land.py customers
"""
import io
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2 import sql

TABLES = ["customers", "stores", "products", "employees", "orders", "order_items"]

EPOCH = datetime(1970, 1, 1)  # "never extracted": first run pulls everything
CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
VOLUME_ROOT = f"/Volumes/{CATALOG}/bronze/landing"


def pg_connect():
    # Inside Docker Compose the host is the service name and the port is the
    # container port (5432). From the codespace shell it is localhost:5433.
    return psycopg2.connect(
        host=os.environ.get("SOURCE_PG_HOST", "walmart-source-db"),
        port=int(os.environ.get("SOURCE_PG_PORT", "5432")),
        dbname=os.environ["SOURCE_PG_DB"],
        user=os.environ["SOURCE_PG_USER"],
        password=os.environ["SOURCE_PG_PASSWORD"],
    )


def ensure_control_table(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS etl_control")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_control.watermark (
            table_name      TEXT PRIMARY KEY,
            last_watermark  TIMESTAMP NOT NULL,
            last_run_at     TIMESTAMPTZ NOT NULL,
            rows_extracted  BIGINT NOT NULL
        )
        """
    )


def get_watermark(cur, table):
    cur.execute("SELECT last_watermark FROM etl_control.watermark WHERE table_name = %s", (table,))
    row = cur.fetchone()
    return row["last_watermark"] if row else EPOCH


def set_watermark(cur, table, watermark, n_rows):
    cur.execute(
        """
        INSERT INTO etl_control.watermark (table_name, last_watermark, last_run_at, rows_extracted)
        VALUES (%s, %s, now(), %s)
        ON CONFLICT (table_name) DO UPDATE
        SET last_watermark = EXCLUDED.last_watermark,
            last_run_at    = EXCLUDED.last_run_at,
            rows_extracted = EXCLUDED.rows_extracted
        """,
        (table, watermark, n_rows),
    )


def extract_and_land(table, run_id=None):
    if table not in TABLES:  # table names go into SQL, so whitelist them
        raise ValueError(f"unknown table: {table}")
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    conn = pg_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            ensure_control_table(cur)
            conn.commit()

            watermark = get_watermark(cur, table)
            cur.execute(
                sql.SQL("SELECT * FROM public.{} WHERE updated_timestamp > %s ORDER BY updated_timestamp").format(
                    sql.Identifier(table)
                ),
                (watermark,),
            )
            rows = cur.fetchall()

        if not rows:
            print(f"[{table}] no changes since {watermark}; nothing to land")
            return None

        new_watermark = max(r["updated_timestamp"] for r in rows)

        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), buf)
        buf.seek(0)

        # Imported here so the Postgres half of this module works without Databricks installed.
        from databricks.sdk import WorkspaceClient

        path = f"{VOLUME_ROOT}/{table}/{table}_{run_id}.parquet"
        WorkspaceClient().files.upload(path, buf, overwrite=True)

        with conn.cursor() as cur:
            set_watermark(cur, table, new_watermark, len(rows))
        conn.commit()

        print(f"[{table}] landed {len(rows)} rows -> {path}; watermark {watermark} -> {new_watermark}")
        return path
    finally:
        conn.close()


if __name__ == "__main__":
    for t in sys.argv[1:] or TABLES:
        extract_and_land(t)