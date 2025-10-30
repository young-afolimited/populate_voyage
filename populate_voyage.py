#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import sys
import logging
import faulthandler
from logging.handlers import RotatingFileHandler
from pathlib import Path

APP_NAME = "populate_voyage"  # change me

# --- version banner (bump every build) ---
APP_NAME = "populate_voyage"
APP_VERSION = "2025-10-30.01"  # CHANGE THIS every rebuild

def log_startup_banner():
    import sys, os, logging, datetime, hashlib
    exe_path = sys.executable if getattr(sys, "frozen", False) else __file__
    # quick hash of the file to prove which binary is running
    try:
        with open(exe_path, "rb") as f:
            data = f.read(256 * 1024)
        h = hashlib.sha256(data).hexdigest()[:16]
    except Exception:
        h = "n/a"
    logging.info("====================================================")
    logging.info("%s %s starting", APP_NAME, APP_VERSION)
    logging.info("Executable: %s", exe_path)
    logging.info("Frozen: %s", getattr(sys, "frozen", False))
    logging.info("Build fingerprint (sha256/first16): %s", h)
    logging.info("Started at: %s", datetime.datetime.now().isoformat(timespec="seconds"))
    logging.info("====================================================")


def _log_path() -> Path:
    """
    Put logs next to the .exe/.py by default.
    If read-only, fallback to user's home dir.
    """
    base = Path(getattr(sys, "_MEIPASS", Path(sys.argv[0]).resolve().parent)).parent \
           if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
    candidate = base / f"{APP_NAME}.log"
    try:
        # Test writeability
        candidate.parent.mkdir(parents=True, exist_ok=True)
        with open(candidate, "a", encoding="utf-8"):
            pass
        return candidate
    except Exception:
        return Path.home() / f"{APP_NAME}.log"

def _configure_logging(verbosity: str = None):
    log_level = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }.get((verbosity or os.getenv("LOG_LEVEL", "INFO")).upper(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Format
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # File handler (rotating)
    fh = RotatingFileHandler(
        _log_path(),
        maxBytes=2_000_000,  # ~2MB
        backupCount=3,
        encoding="utf-8"
    )
    fh.setLevel(log_level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler (if we have a console)
    # When built with PyInstaller --noconsole, there's no visible console,
    # but keeping this handler is harmless.
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Extra: dump low-level crashes (segfaults etc.) to file
    try:
        faulthandler.enable(fh.stream)
    except Exception:
        pass

def _install_global_exception_hook():
    def handle_exception(exc_type, exc, exc_tb):
        logging.critical("Uncaught exception", exc_info=(exc_type, exc, exc_tb))
        _notify_user_failure(str(exc))
        # Non-zero exit for failure
        os._exit(1)
    sys.excepthook = handle_exception

def _notify_user_failure(message: str):
    """
    If we’re on Windows and likely running as --noconsole .exe,
    show a MessageBox so the user sees something.
    Always logs the full traceback already.
    """
    try:
        if sys.platform.startswith("win"):
            import ctypes
            MB_ICONERROR = 0x00000010
            ctypes.windll.user32.MessageBoxW(
                None,
                f"{APP_NAME} failed.\n\n{message}\n\n"
                f"See the log file for details:\n{_log_path()}",
                f"{APP_NAME} Error",
                MB_ICONERROR
            )
    except Exception:
        # Don’t let notification errors crash the app
        pass

def notify_version():
    import sys
    if sys.platform.startswith("win") and getattr(sys, "frozen", False):
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(
                None,
                f"{APP_NAME} {APP_VERSION}\n(see log for path & hash)",
                f"{APP_NAME} build info",
                0x00000040,  # MB_ICONINFORMATION
            )
        except Exception:
            pass

def ensure_path(p) -> Path:
        """Return a pathlib.Path and make sure its parent directory exists."""
        if isinstance(p, str):
            p = Path(p)
        p = p.expanduser()  # handle ~/Documents
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

def main():
    import os, re, unicodedata, tempfile, shutil
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, TimestampType, IntegerType, DecimalType
    )
    from datetime import datetime
    import pandas as pd
    from supabase import create_client
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    spark = (SparkSession.builder
    .appName("WriteToSupabase")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    .getOrCreate())


    csv_path = os.path.expanduser("~/Documents/query_params.csv")


    df = spark.read.csv(str(csv_path), header=False, inferSchema=True)


    # In[3]:


    vault_name = 'akv-1'
    secret_name = 'supabase-service-role-key'
    supabase_url = 'supabase-url'
    credential = DefaultAzureCredential()  # uses Managed Identity or env-creds
    clientSecret = SecretClient(vault_url=f"https://{vault_name}.vault.azure.net", credential=credential)
    key =  clientSecret.get_secret(secret_name).value
    url = clientSecret.get_secret(supabase_url).value


    client = create_client(url, key)

    SCHEMA = "voyage"
    TABLE = "summary"

    print("🔗 Connected to Supabase schema:", SCHEMA, "table:", TABLE)


    # In[4]:


    # Full voyage.summary schema (from your DDL)
    FULL_SCHEMA = StructType([
        StructField("updated_at",           TimestampType(), True),
        StructField("id",                   IntegerType(),   False),
        StructField("voyage_bcr_usd",       DecimalType(18,2), True),
        StructField("voyage_date_voyagestart", TimestampType(), True),
        StructField("voyage_id",            StringType(),    True),
        StructField("voyage_duration_days", DecimalType(10,4), True),
        StructField("voyage_rate_usd_24hrgross", DecimalType(18,2), True),
        StructField("voyage_oh_usd",        DecimalType(18,2), True),
        StructField("voyage_hac_usd",       DecimalType(18,2), True),
        StructField("voyage_cve_usd",       DecimalType(18,2), True),
        StructField("voyage_bnkr_usd",      DecimalType(18,2), True),
        StructField("voyage_bnkr_desc",     StringType(),    True),
        StructField("voyage_chtr_usd_bal",  DecimalType(18,2), True),
        StructField("voyage_expense_usd",   DecimalType(18,2), True),
        StructField("voyage_expense_desc",  StringType(),    True),
        StructField("voyage_bnkr_usd_deduc",DecimalType(18,2), True),
        StructField("created_by",           StringType(),    True),  # uuid → keep as text
        StructField("created_at",           TimestampType(), True),
        StructField("updated_by",           StringType(),    True),  # uuid → keep as text
    ])

    def schema_for_select_cols(select_cols):
        """Return a StructType with only the selected columns, in the same order."""
        # Map field names to fields for quick lookup
        field_map = {f.name: f for f in FULL_SCHEMA.fields}
        fields = []
        for c in select_cols:
            if c in field_map:
                fields.append(field_map[c])
            else:
                # If user asked for a non-schema column, treat as string
                fields.append(StructField(c, StringType(), True))
        return StructType(fields)


    # In[7]:


    # ========= CONFIG =========
    SCHEMA = "voyage"
    TABLE  = "summary"
    DATE_COL = "voyage_date_voyagestart"
    USE_DATE_ONLY = False

    OUT_CSV = os.path.expanduser("~/Documents/db_results.csv")


    SCHEMA_COLS = {
        "updated_at","id","voyage_bcr_usd","voyage_date_voyagestart","voyage_id",
        "voyage_duration_days","voyage_rate_usd_24hrgross","voyage_oh_usd","voyage_hac_usd",
        "voyage_cve_usd","voyage_bnkr_usd","voyage_bnkr_desc","voyage_chtr_usd_bal",
        "voyage_expense_usd","voyage_expense_desc","voyage_bnkr_usd_deduc","created_by",
        "created_at","updated_by"
    }



    pdf = df.toPandas().fillna("")

    n_rows, n_cols = pdf.shape
    print("CSV shape:", n_rows, "rows x", n_cols, "cols")


    # ========= HELPERS =========
    def _norm(s: str) -> str:
        """Normalize text for matching markers (keep underscores)."""
        s = str(s).strip().casefold()
        s = re.sub(r"\s+", "_", s)            # spaces -> underscore
        return re.sub(r"[^0-9a-z_]+", "", s)  # keep a-z, 0-9, _

    def parse_to_iso(val, *, is_end=False):
        """Return 'YYYY-MM-DDTHH:MM:SS' or None. Date-only → 00:00:00 / 23:59:59."""
        if val is None: return None
        s = str(val).strip()
        if not s or s.upper() == "NULL": return None
        if s.upper().startswith("UTC "): s = s[4:].strip()
        if s.endswith("Z"): s = s[:-1]
        s_space = s.replace("T", " ")
        # full datetime
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                dt = datetime.strptime(s_space, fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        # date-only
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                d = datetime.strptime(s_space, fmt)
                return d.strftime("%Y-%m-%dT23:59:59" if is_end else "%Y-%m-%dT00:00:00")
            except ValueError:
                pass
        return None

    def clean_voyage_id(value):
        """Trim, normalize, collapse spaces; keep exact visible chars."""
        if value is None:
            return None
        s = str(value).strip().strip("'").strip('"')
        if not s or s.upper() == "NULL":
            return None
        s = unicodedata.normalize("NFKC", s)
        s = re.sub(r"\s+", " ", s)
        return s

    def discover_marker_cols(pdf, *, anchors=("Header", "StartDate")) -> list[int]:
        """
        Scan the sheet to find which columns contain any of the anchor markers
        (by default 'Header' and 'StartDate'). Return a sorted, de-duped list of
        column indices to use as candidate columns for all marker searches.
        """
        want = {_norm(a) for a in anchors}
        found_cols = set()
        n_rows, n_cols = pdf.shape

        for i in range(n_rows):
            for j in range(n_cols):
                v = pdf.iat[i, j]
                if v is None:
                    continue
                if _norm(v) in want:
                    found_cols.add(j)

        cols = sorted(found_cols)
        if not cols:
            # Fallback: if anchors not found anywhere, search all columns
            cols = list(range(n_cols))
            print("⚠️ No anchor markers found; falling back to all columns.")
        else:
            print(f"✅ Candidate marker columns (from anchors): {cols}")
        return cols


    def find_marker_rc(pdf, marker: str, candidate_cols: list[int] | None = None):
        """
        Find (row, col) for a marker cell, searching ONLY within candidate_cols.
        If candidate_cols is None, they are discovered from 'Header'/'StartDate'.
        Returns (row_index, col_index) or None if not found.
        """
        if candidate_cols is None:
            candidate_cols = discover_marker_cols(pdf, anchors=("Header", "StartDate"))

        tgt = _norm(marker)
        n_rows, _ = pdf.shape

        for i in range(n_rows):
            for j in candidate_cols:
                if j >= len(pdf.columns):  # safety
                    continue
                v = pdf.iat[i, j]
                if v is None:
                    continue
                if _norm(v) == tgt:
                    print(f"✅ Found marker '{marker}' at row={i}, col={j}, value='{v}'")
                    return i, j

        print(f"⚠️ Marker '{marker}' not found in candidate columns {candidate_cols}.")
        return None

    def sanitize_header_names(cols: list[str]) -> list[str]:
        """Lowercase, replace hyphens/spaces with underscore; strip."""
        cleaned = []
        for c in cols:
            if c is None: 
                continue
            s = str(c).strip()
            if not s or s.upper() == "NULL":
                continue
            s = unicodedata.normalize("NFKC", s)
            s = s.lower()
            s = re.sub(r"[-\s]+", "_", s)
            cleaned.append(s)
        return cleaned

    cand_cols = discover_marker_cols(pdf)

    # ========= FIND MARKERS =========
    hdr_rc   = find_marker_rc(pdf, "Header",    candidate_cols=cand_cols)
    start_rc = find_marker_rc(pdf, "StartDate", candidate_cols=cand_cols)
    end_rc   = find_marker_rc(pdf, "EndDate",   candidate_cols=cand_cols)
    vid_rc   = find_marker_rc(pdf, "Voyage ID", candidate_cols=cand_cols)

    print("Markers:",
        "Header", hdr_rc,
        "StartDate", start_rc,
        "EndDate", end_rc,
        "Voyage ID", vid_rc)

    if not hdr_rc:   raise ValueError("Header marker not found")
    if not vid_rc:   raise ValueError("Voyage ID marker not found")

    r_hdr,  c_hdr  = hdr_rc
    r_start, c_start = start_rc if start_rc else (None, None)
    r_end,   c_end   = end_rc   if end_rc   else (None, None)
    r_vid,   c_vid   = vid_rc

    print(f"Markers → Header@({r_hdr},{c_hdr})  Start@({r_start},{c_start})  End@({r_end},{c_end})  VoyageID@({r_vid},{c_vid})")


    # ========= BUILD HEADER COLUMNS (from Header row, to the right) =========
    header_cols_raw = []
    for j in range(c_hdr + 1, n_cols):
        v = pdf.iat[r_hdr, j]
        if v is None: 
            continue
        s = str(v).strip()
        if not s or s.upper() == "NULL":
            continue
        header_cols_raw.append(s)

    header_cols = sanitize_header_names(header_cols_raw)
    if not header_cols:
        raise ValueError("Header row found but no DB column names to the right.")

    # Build select list: header + keys for dedupe
    query_select_cols = header_cols.copy()
    for must in ("voyage_id", DATE_COL):
        if must not in query_select_cols:
            query_select_cols.append(must)

    print("Header (output order):", header_cols)
    print("Query SELECT columns :", query_select_cols)


    # ========= BUILD QUERY SETS PER COLUMN (to the right of markers, same rows) =========
    sets = []
    for j in range(c_vid + 1, n_cols):
        vid_val = pdf.iat[r_vid, j] if r_vid is not None else None
        vid = clean_voyage_id(vid_val)
        if not vid:
            continue

        s_start = parse_to_iso(pdf.iat[r_start, j], is_end=False) if r_start is not None else None
        s_end   = parse_to_iso(pdf.iat[r_end,   j], is_end=True)  if r_end   is not None else None

        sets.append({"id": vid, "start": s_start, "end": s_end})

    print(f"Discovered {len(sets)} set(s):")
    for i, s in enumerate(sets, 1):
        print(f"  {i:02d}. voyage_id={repr(s['id'])}, start={s['start']}, end={s['end']}")


    # ========= OPTIONAL: Smoke test the first ID to verify the DB actually has rows =========
    if sets:
        test_id = sets[0]["id"]
        smoke = client.schema(SCHEMA).table(TABLE) \
            .select(f"voyage_id,{DATE_COL}") \
            .eq("voyage_id", test_id) \
            .limit(3).execute()
        print("SMOKE for first ID:", test_id, "->", len(smoke.data), "rows")
        # If 0 here, the ID text doesn't exist in DB as-is (missing, spacing, or typo).


    # ========= RUN QUERIES & UNION RESULTS =========
    all_rows = []
    page_size = 2000

    for s in sets:
        q = client.schema(SCHEMA).table(TABLE).select(",".join(query_select_cols))

        # Date filters
        if s["start"]:
            q = q.gte(DATE_COL, s["start"][:10] if USE_DATE_ONLY else s["start"])
        if s["end"]:
            q = q.lte(DATE_COL, s["end"][:10] if USE_DATE_ONLY else s["end"])

        # Voyage ID filter
        q = q.eq("voyage_id", s["id"])

        # Optional ordering
        q = q.order(DATE_COL, desc=True, nullsfirst=False)

        # Pagination
        page = 0
        while True:
            resp = q.range(page * page_size, page * page_size + page_size - 1).execute()
            batch = resp.data or []
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            page += 1

    print("Fetched raw rows (pre-dedup):", len(all_rows))


    # ========= PANDAS: DEDUP & WRITE ONLY HEADER COLS =========
    if all_rows:
        pd_df = pd.DataFrame(all_rows)
    else:
        pd_df = pd.DataFrame(columns=query_select_cols)

    # De-dup on unique key pair
    if {"voyage_id", DATE_COL}.issubset(pd_df.columns):
        pd_df = pd_df.drop_duplicates(subset=["voyage_id", DATE_COL])
    elif "voyage_id" in pd_df.columns:
        pd_df = pd_df.drop_duplicates(subset=["voyage_id"])
    else:
        pd_df = pd_df.drop_duplicates()


    if DATE_COL in pd_df.columns:
        _SORT_COL = "__sort_dt__"
        # Try to parse to datetime; rows that can’t parse go to the end
        pd_df[_SORT_COL] = pd.to_datetime(pd_df[DATE_COL], errors="coerce")
        # Primary: date ascending; Secondary (optional): voyage_id for stability
        if "voyage_id" in pd_df.columns:
            pd_df = pd_df.sort_values(by=[_SORT_COL, "voyage_id"], ascending=[True, True], kind="stable")
        else:
            pd_df = pd_df.sort_values(by=[_SORT_COL], ascending=True, kind="stable")
        pd_df = pd_df.drop(columns=[_SORT_COL], errors="ignore")
    else:
        print(f"⚠️ Sort skipped: '{DATE_COL}' not in DataFrame columns:", list(pd_df.columns))

    # Keep ONLY header columns (exact order); create missing cols if needed
    for c in header_cols:
        if c not in pd_df.columns:
            pd_df[c] = ""
    pd_df = pd_df[header_cols]

    # Write safely (avoid Excel lock failures)
    fd, tmp = tempfile.mkstemp(suffix=".csv", dir=os.path.dirname(OUT_CSV))
    os.close(fd)
    pd_df.to_csv(tmp, index=False, encoding="utf-8-sig")
    try:
        shutil.move(tmp, OUT_CSV)
        print("✅ Wrote:", OUT_CSV, "| rows:", len(pd_df), "| columns:", list(pd_df.columns))
    except PermissionError:
        alt = OUT_CSV.replace(".csv", "_new.csv")
        shutil.move(tmp, alt)
        print(f"⚠️ Excel lock detected — saved as '{alt}' instead. Rows: {len(pd_df)}")
    logging.info("Work completed without errors.")
    return 0

if __name__ == "__main__":

    _configure_logging()           # honors LOG_LEVEL env or default INFO
    _install_global_exception_hook()

    try:
        exit_code = main()
        if exit_code == 0:
            logging.info("=== Application success ===")
        else:
            logging.error("=== Application finished with errors (exit_code=%s) ===", exit_code)
            _notify_user_failure(f"Exit code: {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        # Any error not handled inside main() still gets caught here,
        # logged with full traceback, and surfaces to the user.
        logging.exception("Fatal error in top-level execution: %s", e)
        _notify_user_failure(str(e))
        sys.exit(1)
