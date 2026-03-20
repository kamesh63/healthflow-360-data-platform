# 🏥 HealthFlow 360 — Enterprise Healthcare Data Platform

> End-to-end data engineering pipeline processing **11.5 Million healthcare records** using PySpark, Snowflake, and dbt.

---

## 🚀 What This Project Does

A hospital network generates millions of records daily across disconnected systems — patient records, lab results, billing, appointments. This platform unifies them into a single analytics-ready data warehouse with enterprise-grade governance.

---

## 📊 By The Numbers

| | |
|---|---|
| 📦 Records Processed | **11,510,015** |
| 🏥 Source Systems | **5** |
| ⚡ CPU Cores Used | **14** (parallel) |
| ❄️ Snowflake Layers | **3** (Bronze → Silver → Gold) |
| 🔄 dbt Models | **13** |
| ✅ Automated Tests | **45 passing** |
| 🔒 Security Policies | HIPAA-compliant |

---

## 🏗️ Architecture
```
CSV Sources (11.5M rows)
      ↓
PySpark — Schema enforcement, quality checks, deduplication
      ↓
Snowflake RAW — Exact copy of source data
      ↓
Snowflake STAGING — Typed, enriched, business rules applied
      ↓
Snowflake ANALYTICS — Star Schema (4 dims + 3 facts)
      ↓
dbt — Transformations, 45 automated tests, lineage docs
      ↓
Governance — Row Security, Column Masking, Time Travel
      ↓
IDMC — Full pipeline orchestration
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python + Faker |
| Distributed Processing | Apache PySpark 3.5 |
| Cloud Data Warehouse | Snowflake |
| Transformation & Testing | dbt 1.7 |
| Orchestration | IDMC (Informatica) |

---

## 📁 Key Files
```
source_data/generate_data.py      → Generates 11.5M realistic records
spark_pipeline/ingest_pipeline.py → PySpark quality + transformation
snowflake_scripts/01_setup.sql    → 3-layer Snowflake architecture
snowflake_scripts/05_analytics.sql→ Star Schema dimensional model
snowflake_scripts/06_governance.sql→ HIPAA security implementation
dbt_healthflow/models/staging/    → 6 staging models
dbt_healthflow/models/marts/      → 7 mart models (Star Schema)
```

---

## ❄️ Snowflake Data Model
```
DIM_PATIENT (SCD Type 2)
DIM_DOCTOR
DIM_DEPARTMENT      →    FACT_VISITS (3M rows)
DIM_DATE                 FACT_BILLING (3M rows)
                         FACT_LAB_RESULTS (5M rows)
```

---

## 🔒 Governance & Security

- **Row Level Security** — Doctors see only their own patients
- **Column Masking** — Email and DOB masked for non-admin roles
- **Time Travel** — Instant data recovery (tested: 2.9M rows recovered)
- **Streams & Tasks** — Automated incremental processing (CDC)
- **Resource Monitor** — Cost control with credit limits
- **RBAC** — 3 roles: Admin → Developer → Analyst

---

## ⚡ Performance

- PySpark processes 11.5M rows across **14 CPU cores in parallel**
- Snowflake **COPY INTO** loads 128,000 rows/second
- **Clustering keys** on VISIT_YEAR, DEPARTMENT for partition pruning
- **Result cache** — identical queries return instantly

---

## 🏃 How to Run
```bash
# 1. Setup
git clone https://github.com/YOUR_USERNAME/healthflow-360-data-platform
cd healthflow-360-data-platform
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt

# 2. Generate data
python source_data/generate_data.py

# 3. Run PySpark pipeline
python spark_pipeline/ingest_pipeline.py

# 4. Run Snowflake scripts (in order 01 → 06)

# 5. Run dbt
cd dbt_healthflow/healthflow
dbt run && dbt test
```

---

## 👤 Author

**Kamesh V** — Data Engineer, IBM Consulting

---

*Built as IBM Data Engineering Foundations Capstone Project*