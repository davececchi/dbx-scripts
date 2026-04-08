# Databricks notebook source
# MAGIC %md
# MAGIC # Parquet (UC Volume) → Lakebase (Postgres) via COPY (v2 — Executor-Side Load)
# MAGIC
# MAGIC Reads an existing Parquet export from a UC Volume and loads it into Lakebase
# MAGIC using the Postgres `COPY FROM STDIN` protocol. No intermediate CSV files —
# MAGIC data is converted to CSV in-memory in batches.
# MAGIC
# MAGIC **v2 fix:** Each Spark executor opens its own connection to Lakebase and
# MAGIC streams its partition directly via `foreachPartition`. The driver never
# MAGIC touches row data, eliminating driver OOM on large datasets.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks Runtime 13.3 LTS+
# MAGIC - A Lakebase database already provisioned in your workspace
# MAGIC - `psycopg2` installed (pre-installed on DBR 13.3+, or `%pip install psycopg2-binary`)
# MAGIC - Parquet file(s) already exported to a UC Volume
# MAGIC - **Recommended:** Set a native Postgres password on your Lakebase instance
# MAGIC   (via Lakebase UI → Connection Details → Set Password). OAuth tokens expire
# MAGIC   in 60 minutes and large loads can exceed that.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for parameterization
dbutils.widgets.text("parquet_path", "", "Parquet Path in UC Volume (e.g. /Volumes/catalog/schema/volume/path/)")
dbutils.widgets.text("target_table", "", "Target Table Name (in Lakebase)")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host (DNS)")
dbutils.widgets.text("lakebase_port", "5432", "Lakebase Port")
dbutils.widgets.text("lakebase_database", "", "Lakebase Database Name")
dbutils.widgets.text("lakebase_password", "", "Lakebase Password (native PG password — recommended)")
dbutils.widgets.text("batch_size", "100000", "Rows per COPY batch")

# COMMAND ----------

# Read widget values
parquet_path = dbutils.widgets.get("parquet_path")
target_table = dbutils.widgets.get("target_table")
lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_port = dbutils.widgets.get("lakebase_port")
lakebase_database = dbutils.widgets.get("lakebase_database")
batch_size = int(dbutils.widgets.get("batch_size"))

# Password: prefer native PG password; fall back to workspace OAuth token
_pw_widget = dbutils.widgets.get("lakebase_password")
if _pw_widget:
    lakebase_password = _pw_widget
    auth_method = "native Postgres password"
else:
    lakebase_password = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    auth_method = "OAuth token (expires in ~60 min — set a native password for large loads!)"

print(f"Parquet source: {parquet_path}")
print(f"Lakebase:       {lakebase_host}:{lakebase_port}/{lakebase_database}")
print(f"Target table:   {target_table}")
print(f"Batch size:     {batch_size:,} rows")
print(f"Auth method:    {auth_method}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Parquet & Profile Schema

# COMMAND ----------

import time

df = spark.read.parquet(parquet_path)
row_count = df.count()
print(f"Source row count: {row_count:,}")
print(f"Source columns:   {len(df.columns)}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: Repartition for parallel loading
# MAGIC
# MAGIC Each partition gets its own executor-side Postgres connection, so partition
# MAGIC count controls load parallelism. Target ~`batch_size` rows per partition to
# MAGIC keep executor memory reasonable while maximizing throughput.

# COMMAND ----------

num_partitions = max(int(row_count / batch_size), 1)
current_partitions = df.rdd.getNumPartitions()

print(f"Current partitions: {current_partitions}")
print(f"Target partitions:  {num_partitions}")

if num_partitions > current_partitions:
    df = df.repartition(num_partitions)
    print(f"Repartitioned to {num_partitions} partitions.")
elif num_partitions < current_partitions:
    df = df.coalesce(num_partitions)
    print(f"Coalesced to {num_partitions} partitions.")
else:
    print("Partition count already optimal — no change needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Table in Lakebase

# COMMAND ----------

import psycopg2
import io
import csv

# --- Spark type → Postgres type mapping ---
# Reference: https://docs.databricks.com/en/lakebase/manage-data.html
SPARK_TO_PG_TYPE_MAP = {
    # Numeric
    "byte":       "SMALLINT",
    "tinyint":    "SMALLINT",
    "short":      "SMALLINT",
    "smallint":   "SMALLINT",
    "int":        "INTEGER",
    "integer":    "INTEGER",
    "long":       "BIGINT",
    "bigint":     "BIGINT",
    "float":      "REAL",
    "double":     "DOUBLE PRECISION",
    # String
    "string":     "TEXT",
    # Boolean
    "boolean":    "BOOLEAN",
    # Date/Time
    "date":       "DATE",
    "timestamp":  "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP",
    # Binary
    "binary":     "BYTEA",
}


def spark_type_to_pg(spark_type_str: str) -> str:
    """Convert a Spark SQL type string to a Postgres type string."""
    st = spark_type_str.lower().strip()
    if st.startswith("decimal"):
        return st.upper().replace("DECIMAL", "NUMERIC")
    if st.startswith(("array", "map", "struct")):
        return "JSONB"
    pg_type = SPARK_TO_PG_TYPE_MAP.get(st)
    if pg_type is None:
        print(f"  Warning: Unknown Spark type '{spark_type_str}' — defaulting to TEXT")
        return "TEXT"
    return pg_type


def build_create_table_ddl(spark_df, pg_table_name: str) -> str:
    """Build a CREATE TABLE DDL from a Spark DataFrame schema."""
    columns = []
    for field in spark_df.schema.fields:
        pg_type = spark_type_to_pg(field.dataType.simpleString())
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f'    "{field.name}" {pg_type}{nullable}')
    cols_sql = ",\n".join(columns)
    return f'CREATE TABLE IF NOT EXISTS "{pg_table_name}" (\n{cols_sql}\n);'


def get_connection():
    """Create a new psycopg2 connection to Lakebase."""
    return psycopg2.connect(
        host=lakebase_host,
        port=int(lakebase_port),
        dbname=lakebase_database,
        user="databricks",
        password=lakebase_password,
        sslmode="require",
    )


# Build and display the DDL
create_ddl = build_create_table_ddl(df, target_table)
print("Generated DDL:\n")
print(create_ddl)

# COMMAND ----------

# --- Connect to Lakebase and create the table ---
print(f"Connecting to Lakebase at {lakebase_host}:{lakebase_port}/{lakebase_database}...")

conn = get_connection()
conn.autocommit = True
cur = conn.cursor()

# Drop and recreate. Remove the DROP if you want to preserve existing data.
cur.execute(f'DROP TABLE IF EXISTS "{target_table}";')
cur.execute(create_ddl)
print(f"Table '{target_table}' created in Lakebase.")

# Verify columns
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
# MAGIC ## Step 3: Load via foreachPartition (executor-side COPY)
# MAGIC
# MAGIC Each Spark executor opens its own Postgres connection and streams its
# MAGIC partition directly into Lakebase via `COPY FROM STDIN`. The driver never
# MAGIC touches row data — only the executors hold rows in memory.

# COMMAND ----------

import json

columns = [f'"{field.name}"' for field in df.schema.fields]
columns_csv = ", ".join(columns)
copy_sql = f"""COPY "{target_table}" ({columns_csv})
FROM STDIN
WITH (FORMAT csv, HEADER false, DELIMITER ',', NULL '', QUOTE '"', ESCAPE '"');"""

# Identify complex column indices for JSON serialization on executors
complex_col_indices = set(
    i for i, field in enumerate(df.schema.fields)
    if field.dataType.simpleString().startswith(("array", "map", "struct"))
)
complex_col_names = [
    field.name for field in df.schema.fields
    if field.dataType.simpleString().startswith(("array", "map", "struct"))
]

print(f"COPY command:\n{copy_sql}\n")
if complex_col_names:
    print(f"Complex columns (→ JSONB): {complex_col_names}\n")
print(f"Loading via foreachPartition (batch_size={batch_size:,} per COPY)...\n")

# Accumulator to track total rows loaded across all executors
rows_loaded_acc = spark.sparkContext.accumulator(0)

# Capture connection params for executor closures (simple types serialize automatically)
_host = lakebase_host
_port = lakebase_port
_db = lakebase_database
_pw = lakebase_password
_bs = batch_size
_csql = copy_sql
_complex = complex_col_indices


def _copy_partition(rows_iter):
    """Runs on each executor: streams partition rows to Lakebase via COPY."""
    import psycopg2
    import io
    import csv
    import json as _json

    conn = None
    try:
        conn = psycopg2.connect(
            host=_host,
            port=int(_port),
            dbname=_db,
            user="databricks",
            password=_pw,
            sslmode="require",
        )

        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        count = 0
        partition_total = 0

        for row in rows_iter:
            vals = []
            for i, val in enumerate(row):
                if val is None:
                    vals.append("")
                elif i in _complex:
                    vals.append(_json.dumps(val))
                else:
                    vals.append(val)
            writer.writerow(vals)
            count += 1

            if count >= _bs:
                buf.seek(0)
                with conn.cursor() as cur:
                    cur.copy_expert(_csql, buf)
                conn.commit()
                partition_total += count
                count = 0
                buf = io.StringIO()
                writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)

        # Flush remaining rows
        if count > 0:
            buf.seek(0)
            with conn.cursor() as cur:
                cur.copy_expert(_csql, buf)
            conn.commit()
            partition_total += count

        rows_loaded_acc.add(partition_total)
    finally:
        if conn:
            conn.close()


load_start = time.time()
df.foreachPartition(_copy_partition)
load_elapsed = time.time() - load_start

total_rows_loaded = rows_loaded_acc.value

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