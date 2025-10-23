#!/usr/bin/env python
import argparse
import os
import sys
from pathlib import Path

def _imports():
    global pandas, psycopg, SparkSession, openpyxl, pd
    import pandas as pd
    pandas = pd
    import psycopg
    from pyspark.sql import SparkSession
    import openpyxl  # read headers from .xlsm without launching Excel

# ---------------- Excel helpers ----------------
def read_excel_headers(wb_path: Path, sheet: str, header_row: int) -> list[str]:
    import openpyxl
    wb = openpyxl.load_workbook(wb_path, read_only=True, data_only=True, keep_vba=True)
    try:
        ws = wb[sheet]
    except KeyError:
        raise RuntimeError(f"Sheet '{sheet}' not found in {wb_path.name}")
    headers = []
    empty_streak = 0
    col = 1
    # walk until 10 consecutive blanks
    while empty_streak < 10:
        val = ws.cell(row=header_row, column=col).value
        if val is None or str(val).strip() == "":
            empty_streak += 1
            headers.append("")
        else:
            empty_streak = 0
            headers.append(str(val).strip())
        col += 1
    while headers and headers[-1] in ("", None):
        headers.pop()
    headers = [h for h in headers if h not in ("", None)]
    return headers

# ---------------- CSV (query) helpers ----------------
def read_query_params(csv_path: Path) -> "pandas.DataFrame":
    df = pandas.read_csv(csv_path)
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")  # strip empty cols/rows
    return df

def extract_date_range(df: "pandas.DataFrame"):
    """Return (start, end) strings if StartDate/EndDate columns exist; None otherwise.
       Drops StartDate/EndDate from the DataFrame copy used for column filtering."""
    start = None
    end = None
    drop_cols = []
    for name in df.columns:
        lname = str(name).strip().lower()
        if lname == "startdate":
            # pick first non-empty value
            for v in df[name].tolist():
                s = str(v).strip() if (pandas.notna(v) and str(v).strip() != "") else None
                if s:
                    start = s
                    break
            drop_cols.append(name)
        elif lname == "enddate":
            for v in df[name].tolist():
                s = str(v).strip() if (pandas.notna(v) and str(v).strip() != "") else None
                if s:
                    end = s
                    break
            drop_cols.append(name)
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")
    return (start, end), df

def build_where_clause(df_no_dates, date_range, date_column: str):
    """Build WHERE with:
       (optional) date range: date_column BETWEEN %s AND %s
       AND
       (optional) (row1 AND ...) OR (row2 AND ...)
    """
    start, end = date_range
    clauses = []
    params = []

    # date range first (AND ...)
    if start and end:
        clauses.append(f'"{date_column}" BETWEEN %s AND %s')
        params.extend([start, end])
    elif start and not end:
        clauses.append(f'"{date_column}" >= %s')
        params.append(start)
    elif end and not start:
        clauses.append(f'"{date_column}" <= %s')
        params.append(end)

    # row-wise OR groups (excluding StartDate/EndDate)
    row_groups = []
    for _, row in df_no_dates.iterrows():
        sub = []
        for col, val in row.items():
            # skip empties
            if pandas.isna(val) or str(val).strip() == "":
                continue
            v = str(val).strip()
            if any(ch in v for ch in ["%", "_"]):
                sub.append(f'"{col}" LIKE %s')
            else:
                sub.append(f'"{col}" = %s')
        if sub:
            row_groups.append("(" + " AND ".join(sub) + ")")
            # param order mirrors appearance
            for col, val in row.items():
                if pandas.isna(val) or str(val).strip() == "":
                    continue
                params.append(str(val).strip())
    if row_groups:
        # wrap OR group as a single clause and AND it with date if present
        clauses.append("(" + " OR ".join(row_groups) + ")")

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    return where, params

# ---------------- DB I/O ----------------
def fetch_from_supabase(select_cols, where_sql: str, params: list, limit: int = 100000):
    host = os.environ.get("SUPABASE_HOST", "")
    dbname = os.environ.get("SUPABASE_DB", "")
    user = os.environ.get("SUPABASE_USER", "")
    password = os.environ.get("SUPABASE_PASSWORD", "")
    port = int(os.environ.get("SUPABASE_PORT", "5432"))
    schema = os.environ.get("SUPABASE_SCHEMA", "voyage")
    table = os.environ.get("SUPABASE_TABLE", "summary")

    if not (host and dbname and user and password):
        raise RuntimeError("Missing env vars: SUPABASE_HOST, SUPABASE_DB, SUPABASE_USER, SUPABASE_PASSWORD")

    import psycopg
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    # Uncomment if SSL is required by your Supabase instance:
    # conn_str += " sslmode=require"

    if select_cols and len(select_cols) > 0:
        safe_cols = ", ".join([f'"{c}"' for c in select_cols])
    else:
        safe_cols = "*"

    sql = f'SELECT {safe_cols} FROM "{schema}"."{table}" {where_sql} LIMIT {limit}'

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return colnames, rows

# ---------------- Spark + output ----------------
def to_pyspark_and_process(colnames, rows, sort_columns: list[str] | None = None):
    spark = SparkSession.builder.appName("DBData-GetFromDatabase").getOrCreate()
    df = spark.createDataFrame(rows, schema=colnames)
    if sort_columns:
        for c in sort_columns:
            if c in df.columns:
                df = df.orderBy(df[c].desc())
                break
    return df

def write_csv_from_spark(df, out_csv: Path):
    pdf = df.toPandas()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(out_csv, index=False)

# ---------------- Main ----------------
def main():
    parser = argparse.ArgumentParser(description="Query Supabase (voyage.summary) with PySpark and update CSV for Excel DBData sheet")
    parser.add_argument("--workbook", required=True, help="Full path to the Excel workbook (.xlsm)")
    parser.add_argument("--sheet", default="DBData", help="Sheet name to read headers from")
    parser.add_argument("--query_csv", required=True, help="Path to query_params.csv exported by VBA")
    parser.add_argument("--out_csv", required=True, help="Path to write db_results.csv")
    parser.add_argument("--start_row", type=int, default=38, help="Row to begin writing results in Excel (used by VBA importer)")
    parser.add_argument("--use_excel_headers", action="store_true", help="Use headers from Excel row 14 as SELECT columns/order")
    parser.add_argument("--header_row", type=int, default=14, help="Header row index in Excel (default 14)")
    args = parser.parse_args()

    _imports()

    # Determine the date column to apply StartDate/EndDate on
    date_column = os.environ.get("SUPABASE_DATE_COLUMN", "voyage_date_voyagestart")

    # Read Excel headers (if requested) and strip any query-only headers from SELECT
    select_cols = None
    if args.use_excel_headers:
        headers = read_excel_headers(Path(args.workbook), args.sheet, args.header_row)
        # Remove query-only headers from SELECT (they may not be DB columns)
        query_only = {"startdate", "enddate"}
        select_cols = [h for h in headers if h.lower() not in query_only]

    # Build WHERE from the query csv, honoring StartDate/EndDate if provided
    query_df = read_query_params(Path(args.query_csv))
    (start, end), df_no_dates = extract_date_range(query_df)
    where_sql, params = build_where_clause(df_no_dates, (start, end), date_column)

    # Fetch, process, write
    sort_pref = ["voyage_date_voyagestart", "updated_at", "created_at"]
    colnames, rows = fetch_from_supabase(select_cols, where_sql, params)
    spark_df = to_pyspark_and_process(colnames, rows, sort_columns=sort_pref)
    write_csv_from_spark(spark_df, Path(args.out_csv))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)