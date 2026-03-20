# ============================================================
# HealthFlow 360 — Split large CSVs into 200MB chunks
# Snowflake UI limit is 250MB per file
# CONCEPT: Splitting files is standard practice in data engineering
# Hadoop, Spark, and Snowflake all work better with many
# smaller files than one giant file — enables parallelism
# ============================================================

import pandas as pd
import os
import math
import time

BASE    = r"C:\Users\KameshV\Desktop\Learnings\Projects\HealthFlow360\healthflow_360"
CLEANED = os.path.join(BASE, "source_data", "cleaned")
SPLIT   = os.path.join(BASE, "source_data", "split")
os.makedirs(SPLIT, exist_ok=True)

# Files that need splitting (over 250MB)
FILES_TO_SPLIT = {
    "appointments_cleaned.csv": "APPOINTMENTS",
    "billing_cleaned.csv"     : "BILLING",
    "lab_results_cleaned.csv" : "LAB_RESULTS",
}

# Files small enough to upload directly (just copy)
FILES_DIRECT = {
    "departments_cleaned.csv" : "DEPARTMENTS",
    "doctors_cleaned.csv"     : "DOCTORS",
    "patients_cleaned.csv"    : "PATIENTS",
}

CHUNK_SIZE_MB = 190  # Stay safely under 250MB UI limit
CHUNK_ROWS    = 400_000  # Approx rows per chunk

def split_file(filename, table_name):
    filepath = os.path.join(CLEANED, filename)
    size_mb  = os.path.getsize(filepath) / 1e6

    print(f"\n{'='*55}")
    print(f"  Splitting: {filename}")
    print(f"  Size     : {size_mb:.1f} MB")
    print(f"{'='*55}")
    t0 = time.time()

    # Create output folder per table
    out_dir = os.path.join(SPLIT, table_name)
    os.makedirs(out_dir, exist_ok=True)

    # Read in chunks — memory efficient
    # CONCEPT: read_csv with chunksize doesn't load
    # the entire file into RAM — reads piece by piece
    chunk_num   = 1
    total_rows  = 0
    reader      = pd.read_csv(filepath, chunksize=CHUNK_ROWS)

    for chunk in reader:
        out_file = os.path.join(
            out_dir, f"{table_name}_part{chunk_num:03d}.csv"
        )
        # Write header only for first chunk
        chunk.to_csv(out_file, index=False)

        size = os.path.getsize(out_file) / 1e6
        total_rows += len(chunk)
        print(f"  ✅ Part {chunk_num:03d} → {size:.1f} MB  |  {len(chunk):,} rows")
        chunk_num += 1

    print(f"  Total: {total_rows:,} rows in {chunk_num-1} files | {time.time()-t0:.1f}s")
    return chunk_num - 1

def copy_direct(filename, table_name):
    """Copy small files directly into split folder structure"""
    import shutil
    filepath = os.path.join(CLEANED, filename)
    out_dir  = os.path.join(SPLIT, table_name)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{table_name}_part001.csv")
    shutil.copy2(filepath, out_file)
    size = os.path.getsize(filepath) / 1e6
    print(f"  ✅ Copied: {filename} ({size:.1f} MB) → {table_name}/")

if __name__ == "__main__":
    start = time.time()

    print("\n" + "="*55)
    print("  HealthFlow 360 — File Splitter")
    print("  Target: all files under 200MB for Snowflake UI")
    print("="*55)

    # Copy small files directly
    print("\n  📁 Copying small files...")
    for filename, table in FILES_DIRECT.items():
        copy_direct(filename, table)

    # Split large files
    print("\n  ✂️  Splitting large files...")
    for filename, table in FILES_TO_SPLIT.items():
        split_file(filename, table)

    # Summary
    print(f"\n{'='*55}")
    print(f"  ✅ ALL FILES READY FOR UPLOAD")
    print(f"  Location: {SPLIT}")
    print(f"  Upload each folder's files to its matching stage")
    print(f"  Total time: {time.time()-start:.1f}s")
    print(f"{'='*55}")

    # Print upload instructions
    print(f"""
  📋 UPLOAD ORDER (Snowflake UI → Stage):
  ─────────────────────────────────────────
  Folder            → Upload to Stage Path
  ─────────────────────────────────────────
  DEPARTMENTS/      → stage path: DEPARTMENTS/
  DOCTORS/          → stage path: DOCTORS/
  PATIENTS/         → stage path: PATIENTS/
  APPOINTMENTS/     → stage path: APPOINTMENTS/  (multiple files)
  BILLING/          → stage path: BILLING/        (multiple files)
  LAB_RESULTS/      → stage path: LAB_RESULTS/   (multiple files)
  ─────────────────────────────────────────
    """)