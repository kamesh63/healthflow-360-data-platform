# ============================================================
# HealthFlow 360 — PySpark Ingestion & Quality Pipeline
# Reads raw CSVs → validates → cleans → writes cleaned CSVs
# ============================================================

import os
import sys
import time
import glob

# ── Windows Fix: Must be set BEFORE pyspark imports ─────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HADOOP_STUB = os.path.join(BASE, "hadoop_stub")
os.makedirs(os.path.join(HADOOP_STUB, "bin"), exist_ok=True)

os.environ["HADOOP_HOME"]           = HADOOP_STUB
os.environ["hadoop.home.dir"]       = HADOOP_STUB
os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ── Now import PySpark ───────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
from pyspark.sql.window import Window

# ── Paths ────────────────────────────────────────────────────
SRC = os.path.join(BASE, "source_data")
OUT = os.path.join(SRC, "cleaned")
os.makedirs(OUT, exist_ok=True)

# ── SparkSession ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("HealthFlow360_Ingestion") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .config("spark.sql.warehouse.dir",
            os.path.join(BASE, "spark_warehouse").replace("\\", "/")) \
    .config("spark.driver.extraJavaOptions",
            "-Djava.io.tmpdir=" +
            os.path.join(BASE, "spark_tmp").replace("\\", "/")) \
    .getOrCreate()

os.makedirs(os.path.join(BASE, "spark_tmp"), exist_ok=True)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*60)
print("  HealthFlow 360 — PySpark Ingestion Pipeline")
print(f"  Spark Version  : {spark.version}")
print(f"  Cores available: {spark.sparkContext.defaultParallelism}")
print(f"  JAVA_HOME      : {os.environ['JAVA_HOME']}")
print("="*60)

# ============================================================
# SCHEMAS — Explicit schema definition for all 6 tables
# CONCEPT: Never use inferSchema=True in production.
# inferSchema reads the file twice and guesses types.
# Explicit schema = one read, guaranteed types, faster.
# ============================================================

schema_patients = StructType([
    StructField("patient_id",     StringType(),  False),
    StructField("first_name",     StringType(),  True),
    StructField("last_name",      StringType(),  True),
    StructField("dob",            StringType(),  True),
    StructField("gender",         StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("state",          StringType(),  True),
    StructField("zip_code",       StringType(),  True),
    StructField("insurance_type", StringType(),  True),
    StructField("blood_group",    StringType(),  True),
    StructField("phone",          StringType(),  True),
    StructField("email",          StringType(),  True),
    StructField("effective_date", StringType(),  True),
    StructField("expiry_date",    StringType(),  True),
    StructField("is_current",     StringType(),  True),
    StructField("created_at",     StringType(),  True),
])

schema_doctors = StructType([
    StructField("doctor_id",        StringType(),  False),
    StructField("first_name",       StringType(),  True),
    StructField("last_name",        StringType(),  True),
    StructField("specialization",   StringType(),  True),
    StructField("department",       StringType(),  True),
    StructField("qualification",    StringType(),  True),
    StructField("hire_date",        StringType(),  True),
    StructField("experience_years", IntegerType(), True),
    StructField("consultation_fee", DoubleType(),  True),
    StructField("is_active",        StringType(),  True),
    StructField("created_at",       StringType(),  True),
])

schema_departments = StructType([
    StructField("department_id",   StringType(), False),
    StructField("department_name", StringType(), True),
    StructField("floor",           IntegerType(),True),
    StructField("building",        StringType(), True),
    StructField("head_doctor_id",  StringType(), True),
    StructField("created_at",      StringType(), True),
])

schema_appointments = StructType([
    StructField("visit_id",         StringType(),  False),
    StructField("patient_id",       StringType(),  False),
    StructField("doctor_id",        StringType(),  False),
    StructField("department",       StringType(),  True),
    StructField("visit_date",       StringType(),  True),
    StructField("visit_type",       StringType(),  True),
    StructField("diagnosis_code",   StringType(),  True),
    StructField("diagnosis_desc",   StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("follow_up_needed", StringType(),  True),
    StructField("created_at",       StringType(),  True),
])

schema_labs = StructType([
    StructField("lab_id",       StringType(), False),
    StructField("visit_id",     StringType(), True),
    StructField("patient_id",   StringType(), True),
    StructField("department",   StringType(), True),
    StructField("test_name",    StringType(), True),
    StructField("test_date",    StringType(), True),
    StructField("result_value", StringType(), True),
    StructField("unit",         StringType(), True),
    StructField("normal_range", StringType(), True),
    StructField("is_abnormal",  StringType(), True),
    StructField("reviewed_by",  StringType(), True),
    StructField("created_at",   StringType(), True),
])

schema_billing = StructType([
    StructField("billing_id",        StringType(), False),
    StructField("visit_id",          StringType(), True),
    StructField("patient_id",        StringType(), True),
    StructField("billing_date",      StringType(), True),
    StructField("visit_type",        StringType(), True),
    StructField("insurance_type",    StringType(), True),
    StructField("total_amount",      DoubleType(), True),
    StructField("insurance_covered", DoubleType(), True),
    StructField("patient_due",       DoubleType(), True),
    StructField("payment_status",    StringType(), True),
    StructField("payment_method",    StringType(), True),
    StructField("created_at",        StringType(), True),
])

# ============================================================
# UTILITY: Data Quality Reporter
# CONCEPT: Every production pipeline needs quality metrics.
# Measure before AND after cleaning — this is your audit trail.
# ============================================================
def quality_report(df, name, id_col):
    total      = df.count()
    null_ids   = df.filter(F.col(id_col).isNull()).count()
    duplicates = total - df.dropDuplicates([id_col]).count()

    print(f"\n  📊 Quality Report — {name}")
    print(f"  {'─'*45}")
    print(f"  Total rows      : {total:>12,}")
    print(f"  Null primary key: {null_ids:>12,}")
    print(f"  Duplicate IDs   : {duplicates:>12,}")

    null_counts = {}
    for col in df.columns:
        n = df.filter(F.col(col).isNull()).count()
        if n > 0:
            null_counts[col] = n

    if null_counts:
        print(f"  Columns with nulls:")
        for col, cnt in null_counts.items():
            pct = cnt / total * 100
            print(f"    {col:<25}: {cnt:>8,}  ({pct:.1f}%)")
    else:
        print(f"  ✅ No nulls found in any column")

    return {"table": name, "total": total,
            "null_ids": null_ids, "duplicates": duplicates}

# ============================================================
# TABLE-SPECIFIC CLEANING FUNCTIONS
# CONCEPT: Transformations are LAZY in Spark.
# These functions just add steps to the execution plan.
# Nothing runs until toPandas() is called at write time.
# ============================================================

def clean_patients(df):
    return df \
        .withColumn("gender",
            F.when(F.col("gender").isin(["M","F","O"]), F.col("gender"))
             .otherwise("U")) \
        .withColumn("dob", F.to_date("dob", "yyyy-MM-dd")) \
        .withColumn("age",
            F.floor(F.datediff(F.current_date(), F.col("dob")) / 365)) \
        .withColumn("age_group",
            F.when(F.col("age") < 18,  "Pediatric")
             .when(F.col("age") < 40,  "Young Adult")
             .when(F.col("age") < 60,  "Middle Aged")
             .when(F.col("age") < 80,  "Senior")
             .otherwise("Elderly")) \
        .withColumn("effective_date", F.to_date("effective_date", "yyyy-MM-dd"))

def clean_appointments(df):
    return df \
        .withColumn("visit_date",    F.to_date("visit_date", "yyyy-MM-dd")) \
        .withColumn("visit_year",    F.year("visit_date")) \
        .withColumn("visit_month",   F.month("visit_date")) \
        .withColumn("visit_quarter",
            F.concat(F.lit("Q"),
                     F.ceil(F.month("visit_date") / 3).cast("string"))) \
        .withColumn("is_completed",
            F.when(F.col("status") == "Completed", 1).otherwise(0))

def clean_labs(df):
    return df \
        .withColumn("test_date", F.to_date("test_date", "yyyy-MM-dd")) \
        .withColumn("is_abnormal_flag",
            F.when(F.col("is_abnormal") == "Y", 1).otherwise(0))

def clean_billing(df):
    return df \
        .withColumn("billing_date", F.to_date("billing_date", "yyyy-MM-dd")) \
        .withColumn("total_amount",
            F.when(F.col("total_amount") < 0, F.lit(None))
             .otherwise(F.col("total_amount"))) \
        .withColumn("is_paid",
            F.when(F.col("payment_status") == "Paid", 1).otherwise(0)) \
        .withColumn("collection_risk",
            F.when(F.col("payment_status") == "Overdue", "HIGH")
             .when(F.col("payment_status") == "Pending", "MEDIUM")
             .otherwise("LOW"))

# ============================================================
# PIPELINE FUNCTION: process_table
# CONCEPT: DRY principle — one function handles all 6 tables
# Read → Quality check → Clean → Enrich → Write
# ============================================================
def process_table(filename, schema, id_col, clean_fn=None):
    name = filename.replace(".csv", "")
    print(f"\n{'='*60}")
    print(f"  Processing: {filename}")
    print(f"{'='*60}")
    t0 = time.time()

    # ── READ ─────────────────────────────────────────────────
    # CONCEPT: Spark reads lazily — execution plan built here
    # but no data loaded yet until an action is called
    path = os.path.join(SRC, filename)
    df_raw = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(path)

    print(f"  ✓ Schema applied ({len(df_raw.columns)} columns)")

    # ── QUALITY CHECK ─────────────────────────────────────────
    # First action — triggers actual data read
    quality_report(df_raw, f"{name} [RAW]", id_col)

    # ── CLEAN ─────────────────────────────────────────────────
    # Step 1: Drop rows where primary key is null
    df_clean = df_raw.filter(F.col(id_col).isNotNull())

    # Step 2: Deduplicate on primary key keeping first occurrence
    # CONCEPT: Window function — assigns row number per ID group
    # ordered by created_at so we keep the earliest record
    window = Window.partitionBy(id_col).orderBy(F.col("created_at"))
    df_clean = df_clean \
        .withColumn("_rn", F.row_number().over(window)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    # Step 3: Trim whitespace from all string columns
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            df_clean = df_clean.withColumn(
                field.name, F.trim(F.col(field.name))
            )

    # Step 4: Add pipeline audit columns
    # CONCEPT: Every enterprise pipeline stamps these so you
    # always know when and from where data entered the system
    df_clean = df_clean \
        .withColumn("_source_file",    F.lit(filename)) \
        .withColumn("_ingestion_date", F.current_date()) \
        .withColumn("_pipeline_ver",   F.lit("1.0.0"))

    # Step 5: Apply table-specific business rules
    if clean_fn:
        df_clean = clean_fn(df_clean)

    # ── WRITE via Pandas ──────────────────────────────────────
    # CONCEPT: toPandas() is a Spark ACTION — this is the moment
    # ALL lazy transformations above actually execute in parallel
    # across your 14 cores. Everything before this was just a plan.
    out_file = os.path.join(OUT, f"{name}_cleaned.csv")
    df_clean.toPandas().to_csv(out_file, index=False)

    elapsed = time.time() - t0
    size_mb = os.path.getsize(out_file) / 1e6

    print(f"\n  ✅ Written → source_data/cleaned/{name}_cleaned.csv")
    print(f"     Time    : {elapsed:.1f}s")
    print(f"     Size    : {size_mb:.1f} MB")

# ============================================================
# MAIN — Run full pipeline across all 6 tables
# ============================================================
if __name__ == "__main__":
    pipeline_start = time.time()

    process_table("patients.csv",     schema_patients,     "patient_id",    clean_patients)
    process_table("doctors.csv",      schema_doctors,      "doctor_id")
    process_table("departments.csv",  schema_departments,  "department_id")
    process_table("appointments.csv", schema_appointments, "visit_id",      clean_appointments)
    process_table("lab_results.csv",  schema_labs,         "lab_id",        clean_labs)
    process_table("billing.csv",      schema_billing,      "billing_id",    clean_billing)

    # ── Final Summary ─────────────────────────────────────────
    total_time = time.time() - pipeline_start
    total_size = sum(
        os.path.getsize(os.path.join(OUT, f))
        for f in os.listdir(OUT)
        if f.endswith(".csv")
    ) / 1e6

    print(f"\n{'='*60}")
    print(f"  🎉 PIPELINE COMPLETE")
    print(f"  Total time  : {total_time:.1f}s")
    print(f"  Output size : {total_size:.0f} MB")
    print(f"  Tables      : 6")
    print(f"  Location    : {OUT}")
    print(f"{'='*60}\n")

    spark.stop()