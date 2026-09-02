#!/usr/bin/env python3
"""
Parquet → Lakebase (Postgres) via COPY FROM STDIN

Reads Parquet file(s) from a local or mounted path and streams them into a
Postgres-compatible database (e.g. Databricks Lakebase) using the COPY protocol.
Processes data in fixed-size batches to keep memory bounded.

Requirements:
    pip install pyarrow psycopg2-binary
"""

import argparse
import io
import json
import os
import sys
import time

import psycopg2
import pyarrow as pa
import pyarrow.dataset as ds


# ---------------------------------------------------------------------------
# Arrow type → Postgres type mapping
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

def build_create_table_ddl(schema: pa.Schema, table_name: str) -> str:
    columns = []
    for field in schema:
        pg_type = arrow_type_to_pg(field.type)
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f'    "{field.name}" {pg_type}{nullable}')
    cols_sql = ",\n".join(columns)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n{cols_sql}\n);'


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Stream Parquet file(s) into Postgres/Lakebase via COPY.",
    )
    p.add_argument("parquet_path", help="Path to a Parquet file or directory of Parquet files")
    p.add_argument("--table", required=True, help="Target table name in Postgres")
    p.add_argument("--host", required=True, help="Postgres host")
    p.add_argument("--port", type=int, default=5432, help="Postgres port (default: 5432)")
    p.add_argument("--database", required=True, help="Postgres database name")
    p.add_argument("--user", default="databricks", help="Postgres user (default: databricks)")
    p.add_argument("--password", default=None,
                   help="Postgres password (default: reads PGPASSWORD env var)")
    p.add_argument("--batch-size", type=int, default=100_000,
                   help="Rows per COPY batch (default: 100000)")
    p.add_argument("--no-drop", action="store_true",
                   help="Don't DROP the target table before loading")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        print("Error: provide --password or set the PGPASSWORD environment variable.",
              file=sys.stderr)
        sys.exit(1)

    def get_connection():
        return psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.database,
            user=args.user,
            password=password,
            sslmode="require",
        )

    # ---- Step 1: Read dataset metadata ----
    dataset = ds.dataset(args.parquet_path, format="parquet")
    schema = dataset.schema
    row_count = dataset.count_rows()

    print(f"Parquet source: {args.parquet_path}")
    print(f"Target:         {args.host}:{args.port}/{args.database} → {args.table}")
    print(f"Source rows:    {row_count:,}")
    print(f"Source columns: {len(schema)}")
    print(f"Batch size:     {args.batch_size:,}")
    print()
    for field in schema:
        print(f"  {field.name:30s} {field.type}")
    print()

    # ---- Step 2: Create target table ----
    create_ddl = build_create_table_ddl(schema, args.table)
    print(f"Generated DDL:\n{create_ddl}\n")

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    if not args.no_drop:
        cur.execute(f'DROP TABLE IF EXISTS "{args.table}";')
    cur.execute(create_ddl)
    print(f"Table '{args.table}' ready.\n")

    cur.close()
    conn.close()

    # ---- Step 3: Stream batches via COPY ----
    columns = [f'"{field.name}"' for field in schema]
    columns_csv = ", ".join(columns)
    copy_sql = (
        f'COPY "{args.table}" ({columns_csv}) '
        f"FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',', "
        f"NULL '', QUOTE '\"', ESCAPE '\"');"
    )

    complex_cols = [f.name for f in schema if is_complex_type(f.type)]
    if complex_cols:
        print(f"Complex columns (→ JSONB): {complex_cols}")

    print(f"Streaming...\n")

    load_start = time.time()
    total_rows_loaded = 0
    batch_num = 0

    conn = get_connection()

    import csv
    complex_col_set = set(complex_cols)
    col_names = [f.name for f in schema]

    for batch in dataset.to_batches(batch_size=args.batch_size):
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

    # ---- Verification ----
    conn_verify = get_connection()
    with conn_verify.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{args.table}";')
        pg_count = cur.fetchone()[0]
    conn_verify.close()
    conn.close()

    print(f"\n=== Final Results ===")
    print(f"  Source rows (Parquet):   {row_count:,}")
    print(f"  Target rows (Postgres):  {pg_count:,}")
    print(f"  Load time:    {load_elapsed:.1f}s")
    if row_count == pg_count:
        print(f"  Row counts MATCH")
    else:
        print(f"  MISMATCH — difference of {abs(row_count - pg_count):,} rows")
        sys.exit(1)


if __name__ == "__main__":
    main()
