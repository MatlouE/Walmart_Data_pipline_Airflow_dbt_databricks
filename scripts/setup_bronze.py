"""Create the bronze schema and the landing volume in Unity Catalog.

Idempotent: safe to re-run. Catalog comes from DATABRICKS_CATALOG in .env
(or the first CLI argument). Uses the Unity Catalog REST API through the SDK,
so no SQL warehouse is needed for this step.
"""
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import VolumeType
from dotenv import load_dotenv

load_dotenv()

CATALOG = sys.argv[1] if len(sys.argv) > 1 else os.environ["DATABRICKS_CATALOG"]
SCHEMA = "bronze"
VOLUME = "landing"

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

try:
    w.schemas.create(name=SCHEMA, catalog_name=CATALOG, comment="Raw Delta tables loaded by Auto Loader")
    print(f"created schema {CATALOG}.{SCHEMA}")
except AlreadyExists:
    print(f"schema {CATALOG}.{SCHEMA} already exists")

try:
    w.volumes.create(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name=VOLUME,
        volume_type=VolumeType.MANAGED,
        comment="Landing zone for files pushed from Airflow",
    )
    print(f"created volume {CATALOG}.{SCHEMA}.{VOLUME}")
except AlreadyExists:
    print(f"volume {CATALOG}.{SCHEMA}.{VOLUME} already exists")

# Round-trip check: upload and delete a tiny file through the Files API,
# which is exactly what the Airflow extract task will use.
import io

probe = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/_probe/ping.txt"
w.files.upload(probe, io.BytesIO(b"ok"), overwrite=True)
w.files.delete(probe)
print(f"upload/delete round-trip OK under /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/")