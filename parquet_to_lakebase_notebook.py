# Databricks notebook source
# MAGIC %md
# MAGIC # Parquet (UC Volume) → Lakebase (Postgres) via PyArrow + COPY
# MAGIC
# MAGIC Reads Parquet file(s) from a UC Volume using PyArrow (no Spark) and streams
# MAGIC them into Lakebase via the Postgres `COPY FROM STDIN` protocol. Processes
# MAGIC data in fixed-size batches to keep driver memory bounded.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks Runtime 13.3 LTS+ (pyarrow & psycopg2 pre-installed)
# MAGIC - A Lakebase database already provisioned in your workspace
# MAGIC - Parquet file(s) in a UC Volume
# MAGIC - **Recommended:** Set a native Postgres password on your Lakebase instance
# MAGIC   (via Lakebase UI → Connection Details → Set Password). OAuth tokens expire
# MAGIC   in 60 minutes and large loads can exceed that.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("parquet_path", "", "Parquet Path in UC Volume (e.g. /Volumes/catalog/schema/volume/path/)")
dbutils.widgets.text("target_table", "", "Target Table Name (in Lakebase)")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host (DNS)")
dbutils.widgets.text("lakebase_port", "5432", "Lakebase Port")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "Lakebase Database Name")
dbutils.widgets.text("lakebase_instance", "", "Lakebase Instance Name (for OAuth — leave blank if using native password)")
dbutils.widgets.text("lakebase_password", "", "Lakebase Password (native PG password — leave blank to use OAuth)")
dbutils.widgets.text("batch_size", "100000", "Rows per COPY batch")

# COMMAND ----------

parquet_path = dbutils.widgets.get("parquet_path")
target_table = dbutils.widgets.get("target_table")
lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_port = dbutils.widgets.get("lakebase_port")
lakebase_database = dbutils.widgets.get("lakebase_database")
batch_size = int(dbutils.widgets.get("batch_size"))

# Password: prefer native PG password; fall back to Lakebase OAuth credential
_pw_widget = dbutils.widgets.get("lakebase_password")
_instance_widget = dbutils.widgets.get("lakebase_instance")
if _pw_widget:
    lakebase_password = _pw_widget
    auth_method = "native Postgres password"
else:
    assert _instance_widget, (
        "Provide either lakebase_password (native PG) or lakebase_instance (for OAuth)."
    )
    import requests, uuid
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _api_token = _ctx.apiToken().get()
    _api_url = _ctx.apiUrl().get()
    _resp = requests.post(
        f"{_api_url}/api/2.0/database/credentials",
        headers={"Authorization": f"Bearer {_api_token}"},
        json={"instance_names": [_instance_widget], "request_id": str(uuid.uuid4())},
    )
    _resp.raise_for_status()
    lakebase_password = _resp.json()["token"]
    auth_method = "Lakebase OAuth credential (expires in ~60 min)"

# Auto-detect the current user's email for the PG connection
lakebase_user = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().userName().get()
)

print(f"Parquet source: {parquet_path}")
print(f"Lakebase:       {lakebase_host}:{lakebase_port}/{lakebase_database}")
print(f"Target table:   {target_table}")
print(f"Batch size:     {batch_size:,} rows")
print(f"PG user:        {lakebase_user}")
print(f"Auth method:    {auth_method}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Parquet & Profile Schema (PyArrow)

# COMMAND ----------

import time
import pyarrow as pa
import pyarrow.dataset as ds

dataset = ds.dataset(parquet_path, format="parquet")
schema = dataset.schema
row_count = dataset.count_rows()

print(f"Source row count: {row_count:,}")
print(f"Source columns:   {len(schema)}")
print()
for field in schema:
    print(f"  {field.name:30s} {field.type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Table in Lakebase

# COMMAND ----------

import psycopg2
import io
import csv
import json


def arrow_type_to_pg(arrow_type) -> str:
    """Convert a PyArrow type to a Postgres type string."""
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return "SMALLINT"
    if pa.types.is_int32(arrow_type):
        return "INTEGER"
    if pa.types.is_int64(arrow_type):
        return "BIGINT"
    if pa.types.is_float32(arrow_type):
        return "REAL"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE PRECISION"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "TEXT"
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "BYTEA"
    if (pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)
            or pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type)):
        return "JSONB"
    print(f"  Warning: Unknown Arrow type '{arrow_type}' — defaulting to TEXT")
    return "TEXT"


def is_complex_type(arrow_type) -> bool:
    return (pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)
            or pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type))


def build_create_table_ddl(schema: pa.Schema, table_name: str) -> str:
    columns = []
    for field in schema:
        pg_type = arrow_type_to_pg(field.type)
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f'    "{field.name}" {pg_type}{nullable}')
    cols_sql = ",\n".join(columns)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n{cols_sql}\n);'


def get_connection():
    return psycopg2.connect(
        host=lakebase_host,
        port=int(lakebase_port),
        dbname=lakebase_database,
        user=lakebase_user,
        password=lakebase_password,
        sslmode="require",
    )


create_ddl = build_create_table_ddl(schema, target_table)
print("Generated DDL:\n")
print(create_ddl)

# COMMAND ----------

print(f"Connecting to Lakebase at {lakebase_host}:{lakebase_port}/{lakebase_database}...")

conn = get_connection()
conn.autocommit = True
cur = conn.cursor()

# Drop and recreate. Remove the DROP if you want to preserve existing data.
cur.execute(f'DROP TABLE IF EXISTS "{target_table}";')
cur.execute(create_ddl)
print(f"Table '{target_table}' created in Lakebase.")

cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position;
""", (target_table,))
print("\nTarget table columns:")
for row in cur.fetchall():
    print(f"  {row[0]:30s} {row[1]}")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Stream Parquet → Lakebase via COPY FROM STDIN
# MAGIC
# MAGIC Uses `pyarrow.dataset.to_batches()` to stream fixed-size batches from the
# MAGIC Parquet files. Each batch is converted to CSV in-memory and sent via
# MAGIC `COPY FROM STDIN`. No Spark involved — runs entirely on the driver.

# COMMAND ----------

columns = [f'"{field.name}"' for field in schema]
columns_csv = ", ".join(columns)
copy_sql = (
    f'COPY "{target_table}" ({columns_csv}) '
    f"FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',', "
    f"NULL '', QUOTE '\"', ESCAPE '\"');"
)

complex_cols = [f.name for f in schema if is_complex_type(f.type)]
complex_col_set = set(complex_cols)
col_names = [f.name for f in schema]

print(f"COPY command:\n{copy_sql}\n")
if complex_cols:
    print(f"Complex columns (→ JSONB): {complex_cols}\n")
print(f"Streaming in batches of {batch_size:,} rows...\n")

load_start = time.time()
total_rows_loaded = 0
batch_num = 0

conn = get_connection()

for batch in dataset.to_batches(batch_size=batch_size):
    batch_num += 1
    rows = batch.to_pylist()

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        vals = []
        for col_name in col_names:
            val = row[col_name]
            if val is None:
                vals.append("")
            elif col_name in complex_col_set:
                vals.append(json.dumps(val))
            else:
                vals.append(val)
        writer.writerow(vals)
    buf.seek(0)

    try:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, buf)
        conn.commit()
        total_rows_loaded += len(rows)
        elapsed = time.time() - load_start
        pct = total_rows_loaded / row_count * 100 if row_count > 0 else 0
        print(
            f"  Batch {batch_num}: {len(rows):,} rows — "
            f"{total_rows_loaded:,} total ({pct:.1f}%) — "
            f"{elapsed:.1f}s elapsed"
        )
    except Exception as e:
        print(f"  Batch {batch_num}: FAILED — {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        conn = get_connection()

load_elapsed = time.time() - load_start

print(f"\nLoad complete in {load_elapsed:.1f}s")
print(f"  Rows streamed: {total_rows_loaded:,}")
if load_elapsed > 0:
    print(f"  Avg rate:      {total_rows_loaded / load_elapsed:,.0f} rows/s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

conn = get_connection()
with conn.cursor() as cur:
    cur.execute(f'SELECT COUNT(*) FROM "{target_table}";')
    pg_count = cur.fetchone()[0]
conn.close()

print(f"=== Final Results ===")
print(f"  Source rows (Parquet):    {row_count:,}")
print(f"  Target rows (Lakebase):  {pg_count:,}")
print(f"  Load time:    {load_elapsed:.1f}s")
if row_count == pg_count:
    print(f"  Row counts MATCH")
else:
    print(f"  MISMATCH — difference of {abs(row_count - pg_count):,} rows")
