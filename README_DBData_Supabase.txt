DBData ↔ Supabase (PySpark) — Updated for StartDate/EndDate
==========================================================

What changed
------------
- The Python now recognizes **StartDate** and **EndDate** in your **row 14** header line (or exported CSV).
- Those are treated as **query-only** headers (not selected from the DB) and used to build a **date range** filter.
- The date range is applied to a target column controlled by `SUPABASE_DATE_COLUMN` (default: `voyage_date_voyagestart`).

How the WHERE is built
----------------------
- If StartDate and EndDate are present (and non-empty), we add:
  - `"${SUPABASE_DATE_COLUMN}" BETWEEN %s AND %s`
- If only StartDate: `"${SUPABASE_DATE_COLUMN}" >= %s`
- If only EndDate: `"${SUPABASE_DATE_COLUMN}" <= %s`
- Your normal row-wise filters (rows 8–10) are combined as OR groups, and combined with the date clause using AND.

Example (conceptual):
```
WHERE ("voyage_date_voyagestart" BETWEEN '2025-07-01' AND '2025-08-01')
  AND ((voyage_id = 'BAL22') OR (vessel_name LIKE '%BLUE%'))
```

Selecting and ordering columns
------------------------------
- With `--use_excel_headers --header_row 14`, the script **reads your header row** on `DBData` and uses those as SELECT columns and order.
- It automatically **omits** `StartDate` / `EndDate` from SELECT (assuming they’re just query parameters).
- Results CSV matches the order of the remaining headers.

Windows reminders
-----------------
- Keep the script next to your workbook (e.g., `250603_BAL22_db_YNMod_v1.xlsm`).
- Create a venv and install requirements:
  ```powershell
  python -m venv venv
  .env\Scripts\pip install -r .equirements.txt
  ```
- Set environment variables:
  ```powershell
  $env:SUPABASE_HOST="db.YOUR-PROJECT.supabase.co"
  $env:SUPABASE_DB="postgres"
  $env:SUPABASE_USER="postgres"
  $env:SUPABASE_PASSWORD="YOUR_PASSWORD"
  # optional
  $env:SUPABASE_PORT="5432"
  $env:SUPABASE_SCHEMA="voyage"
  $env:SUPABASE_TABLE="summary"
  $env:SUPABASE_DATE_COLUMN="voyage_date_voyagestart"
  ```

VBA modules
-----------
Your existing split modules keep working as-is:
- `DBData_Export.bas` — exports query CSV (rows 8–10 under row 14 headers)
- `DBData_RunPython.bas` — runs Python with `--use_excel_headers --header_row 14`
- `DBData_Import.bas` — imports `db_results.csv` at row 38

Tip: after updating the header row to include `StartDate` / `EndDate`, just rerun:
1. `Export_DB_Query_CSV`
2. `Run_DB_Python_Query`
3. `Import_DB_Results_CSV`