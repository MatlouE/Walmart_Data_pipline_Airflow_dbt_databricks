import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

DATA_DIR = Path(__file__).parent / "data"

# Parents first, children last (foreign keys)
LOAD_ORDER = ["customers", "stores", "products", "employees", "orders", "order_items"]


def connect():
    return psycopg2.connect(
        host=os.environ.get("SOURCE_DB_HOST", "localhost"),
        port=os.environ.get("SOURCE_DB_PORT", "5433"),
        dbname=os.environ["SOURCE_DB_NAME"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
    )


def load_table(cur, table):
    path = DATA_DIR / f"{table}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing CSV: {path}")
    with open(path, newline="") as f:
        columns = f.readline().strip().split(",")
        f.seek(0)
        query = sql.SQL(
            "COPY public.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        cur.copy_expert(query.as_string(cur), f)


def main():
    conn = connect()
    try:
        # One transaction: commits if everything succeeds, rolls back on any error
        with conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE {} CASCADE").format(
                    sql.SQL(", ").join(sql.Identifier(t) for t in LOAD_ORDER)
                )
            )
            for table in LOAD_ORDER:
                load_table(cur, table)
                cur.execute(sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(table)))
                print(f"{table}: {cur.fetchone()[0]} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()