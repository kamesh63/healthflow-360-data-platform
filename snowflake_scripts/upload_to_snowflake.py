# ============================================================
# HealthFlow 360 — Upload cleaned CSVs to Snowflake Stage
# Then COPY INTO RAW tables
# ============================================================

import snowflake.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

# ── Connection ───────────────────────────────────────────────
# CONCEPT: We read credentials from .env file — never hardcode
# passwords in scripts. This is security best practice.
BASE     = r"C:\Users\KameshV\Desktop\Learnings\Projects\HealthFlow360\healthflow_360"
CLEANED  = os.path.join(BASE, "source_data", "cleaned")

# Fill in your Snowflake credentials
SNOWFLAKE_CONFIG = {
    "account"  : "YOUR_SNOWFLAKE_ACCOUNT",
    "user"     : "user",
    "password" : "mypassword",
    "warehouse": "HEALTHFLOW_WH",
    "database" : "HEALTHFLOW_DB",
    "schema"   : "RAW",
    "role"     : "ACCOUNTADMIN"
}

# ── File to Table mapping ─────────────────────────────────────
FILE_TABLE_MAP = {
    "patients_cleaned.csv"    : "PATIENTS",
    "doctors_cleaned.csv"     : "DOCTORS",
    "departments_cleaned.csv" : "DEPARTMENTS",
    "appointments_cleaned.csv": "APPOINTMENTS",
    "lab_results_cleaned.csv" : "LAB_RESULTS",
    "billing_cleaned.csv"     : "BILLING",
}

def connect():
    print("\n  Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print(f"  ✅ Connected — Account: {SNOWFLAKE_CONFIG['account']}")
    return conn

def upload_and_load(conn):
    cur = conn.cursor()
    total_start = time.time()

    print("\n" + "="*60)
    print("  HealthFlow 360 — Snowflake Data Load")
    print("="*60)

    # Use correct context
    cur.execute("USE WAREHOUSE HEALTHFLOW_WH")
    cur.execute("USE DATABASE HEALTHFLOW_DB")
    cur.execute("USE SCHEMA RAW")

    for filename, table in FILE_TABLE_MAP.items():
        filepath = os.path.join(CLEANED, filename)

        if not os.path.exists(filepath):
            print(f"\n  ⚠️  Skipping {filename} — file not found")
            continue

        file_size = os.path.getsize(filepath) / 1e6
        print(f"\n{'='*60}")
        print(f"  Loading: {filename} ({file_size:.1f} MB) → {table}")
        print(f"{'='*60}")
        t0 = time.time()

        # ── STEP 1: PUT — upload file to internal stage ───────
        # CONCEPT: PUT compresses and encrypts your file
        # then uploads it to Snowflake's internal storage.
        # The @~ means your personal user stage.
        # @HEALTHFLOW_RAW_STAGE means the named stage we created.
        print(f"  📤 Uploading to stage...")
        put_sql = f"""
            PUT file://{filepath.replace(chr(92), '/')}
            @HEALTHFLOW_RAW_STAGE/{table}/
            AUTO_COMPRESS = TRUE
            OVERWRITE     = TRUE
            PARALLEL      = 4
        """
        cur.execute(put_sql)
        put_result = cur.fetchall()
        put_status = put_result[0][6] if put_result else "UNKNOWN"
        print(f"  ✅ Upload status: {put_status}")

        # ── STEP 2: COPY INTO — load from stage to table ──────
        # CONCEPT: COPY INTO is Snowflake's bulk loader.
        # It reads files in parallel from the stage,
        # applies the file format, and inserts into the table.
        # ON_ERROR = CONTINUE means bad rows are skipped
        # (logged in load history) rather than failing everything.
        print(f"  📥 Running COPY INTO {table}...")
        copy_sql = f"""
            COPY INTO RAW.{table}
            FROM @HEALTHFLOW_RAW_STAGE/{table}/
            FILE_FORMAT = (FORMAT_NAME = 'HEALTHFLOW_CSV_FORMAT')
            ON_ERROR    = 'CONTINUE'
            PURGE       = FALSE
        """
        cur.execute(copy_sql)
        copy_results = cur.fetchall()

        # Parse results
        total_rows  = sum(r[3] for r in copy_results if r[3])
        error_rows  = sum(r[5] for r in copy_results if r[5])
        elapsed     = time.time() - t0

        print(f"  ✅ COPY INTO complete")
        print(f"     Rows loaded : {total_rows:>10,}")
        print(f"     Rows errored: {error_rows:>10,}")
        print(f"     Time        : {elapsed:.1f}s")

    # ── STEP 3: Verify row counts ─────────────────────────────
    print(f"\n{'='*60}")
    print(f"  📊 Final Row Count Verification")
    print(f"{'='*60}")

    tables = ["PATIENTS","DOCTORS","DEPARTMENTS",
              "APPOINTMENTS","LAB_RESULTS","BILLING"]
    total_loaded = 0

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM RAW.{table}")
        count = cur.fetchone()[0]
        total_loaded += count
        print(f"  RAW.{table:<20}: {count:>12,} rows")

    print(f"  {'─'*45}")
    print(f"  {'TOTAL':<25}: {total_loaded:>12,} rows")
    print(f"\n  Total time: {time.time()-total_start:.1f}s")
    print(f"{'='*60}\n")

    cur.close()

if __name__ == "__main__":
    conn = connect()
    upload_and_load(conn)
    conn.close()
    print("  Connection closed. Load complete! 🎉")