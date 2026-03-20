# HealthFlow 360 — Synthetic Healthcare Data Generator
# Generates ~11.5 million realistic hospital records

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import time

fake = Faker('en_US')
Faker.seed(42)  # seed = reproducible data every run
np.random.seed(42)
random.seed(42)

# Output folder
OUTPUT_DIR = os.path.join(os.path.dirname(__file__))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# CONCEPT: Constants as single source of truth
# Change numbers here → everything else adjusts automatically
NUM_PATIENTS     = 500_000
NUM_DOCTORS      = 10_000
NUM_APPOINTMENTS = 3_000_000
NUM_LABS         = 5_000_000
NUM_BILLING      = 3_000_000
START_DATE       = datetime(2021, 1, 1)
END_DATE         = datetime(2024, 12, 31)

# Realistic reference data
DEPARTMENTS = [
    "Cardiology", "Neurology", "Orthopedics", "General Medicine",
    "Radiology", "Oncology", "Pediatrics", "Emergency",
    "Gynecology", "Psychiatry", "Dermatology", "Nephrology",
    "Gastroenterology", "Pulmonology", "Endocrinology"
]

# Department weights: Emergency & General Medicine see more patients
DEPT_WEIGHTS = [0.10, 0.07, 0.08, 0.18, 0.06, 0.05, 0.09,
                0.15, 0.06, 0.04, 0.04, 0.03, 0.03, 0.01, 0.01]

INSURANCE_TYPES = ["Private", "Medicare", "Medicaid", "Uninsured", "Government"]
INSURANCE_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]

VISIT_TYPES = ["OPD", "IPD", "Emergency", "Telehealth"]
VISIT_WEIGHTS = [0.55, 0.20, 0.15, 0.10]

STATES_CITIES = {
    "IL": ["Chicago", "Aurora", "Naperville"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
    "CA": ["Los Angeles", "San Francisco", "San Diego"],
    "NY": ["New York", "Buffalo", "Albany"],
    "FL": ["Miami", "Orlando", "Tampa"],
    "OH": ["Columbus", "Cleveland", "Cincinnati"],
    "PA": ["Philadelphia", "Pittsburgh", "Allentown"],
    "AZ": ["Phoenix", "Tucson", "Scottsdale"],
}

# Diagnosis codes by department — realistic ICD-10 codes
DIAGNOSES = {
    "Cardiology":       [("I21","Acute MI"), ("I50","Heart Failure"),
                         ("I10","Hypertension"), ("I48","Atrial Fibrillation"),
                         ("I25","Chronic Ischemic Heart Disease")],
    "Neurology":        [("G43","Migraine"), ("G35","Multiple Sclerosis"),
                         ("G20","Parkinson's Disease"), ("G40","Epilepsy"),
                         ("I63","Cerebral Infarction")],
    "Orthopedics":      [("M54","Low Back Pain"), ("S72","Femur Fracture"),
                         ("M17","Knee Osteoarthritis"), ("S42","Shoulder Fracture"),
                         ("M75","Shoulder Lesion")],
    "General Medicine": [("J06","Upper Respiratory"), ("E11","Type 2 Diabetes"),
                         ("J18","Pneumonia"), ("K29","Gastritis"),
                         ("R50","Fever")],
    "Radiology":        [("Z12","Screening"), ("Z00","General Exam"),
                         ("R91","Lung Mass"), ("R93","Abnormal Findings"),
                         ("Z13","Encounter for Screening")],
    "Oncology":         [("C50","Breast Cancer"), ("C34","Lung Cancer"),
                         ("C18","Colon Cancer"), ("C61","Prostate Cancer"),
                         ("C92","Myeloid Leukemia")],
    "Pediatrics":       [("J45","Asthma"), ("H66","Otitis Media"),
                         ("A09","Gastroenteritis"), ("L20","Atopic Dermatitis"),
                         ("J06","Upper Respiratory")],
    "Emergency":        [("S09","Head Injury"), ("T14","Trauma"),
                         ("R07","Chest Pain"), ("R55","Syncope"),
                         ("S00","Superficial Injury")],
    "Gynecology":       [("N94","Pelvic Pain"), ("O26","Pregnancy Complication"),
                         ("N92","Irregular Menstruation"), ("C53","Cervical Cancer"),
                         ("N80","Endometriosis")],
    "Psychiatry":       [("F32","Major Depression"), ("F41","Anxiety"),
                         ("F20","Schizophrenia"), ("F31","Bipolar Disorder"),
                         ("F10","Alcohol Disorder")],
    "Dermatology":      [("L40","Psoriasis"), ("L50","Urticaria"),
                         ("C43","Melanoma"), ("L30","Dermatitis"),
                         ("L60","Nail Disorder")],
    "Nephrology":       [("N18","Chronic Kidney Disease"), ("N20","Kidney Stone"),
                         ("N39","UTI"), ("N04","Nephrotic Syndrome"),
                         ("N17","Acute Kidney Failure")],
    "Gastroenterology": [("K57","Diverticular Disease"), ("K50","Crohn's Disease"),
                         ("K21","GERD"), ("K80","Cholelithiasis"),
                         ("K70","Alcoholic Liver Disease")],
    "Pulmonology":      [("J44","COPD"), ("J45","Asthma"), ("J18","Pneumonia"),
                         ("J84","Pulmonary Fibrosis"), ("J93","Pneumothorax")],
    "Endocrinology":    [("E11","Type 2 Diabetes"), ("E03","Hypothyroidism"),
                         ("E05","Hyperthyroidism"), ("E27","Adrenal Disorder"),
                         ("E66","Obesity")],
}

# Lab tests by department
LAB_TESTS = {
    "Cardiology":       [("Troponin","ng/mL","0.0-0.4"),
                         ("BNP","pg/mL","0-100"),
                         ("Lipid Panel","mg/dL","0-200"),
                         ("ECG","text","Normal")],
    "Neurology":        [("MRI Brain","text","Normal"),
                         ("EEG","text","Normal"),
                         ("CSF Analysis","cells/μL","0-5")],
    "Orthopedics":      [("X-Ray","text","Normal"),
                         ("MRI Spine","text","Normal"),
                         ("Bone Density","g/cm²","1.0-1.4")],
    "General Medicine": [("CBC","g/dL","13.5-17.5"),
                         ("HbA1c","%","4.0-5.6"),
                         ("CMP","mg/dL","70-100"),
                         ("Urine Analysis","text","Normal")],
    "Radiology":        [("CT Scan","text","Normal"),
                         ("Mammogram","text","Normal"),
                         ("PET Scan","text","Normal")],
    "Oncology":         [("Tumor Markers","U/mL","0-35"),
                         ("Biopsy","text","Benign"),
                         ("CBC","g/dL","13.5-17.5")],
    "Pediatrics":       [("CBC","g/dL","11.5-15.5"),
                         ("Throat Culture","text","Negative"),
                         ("Urine Analysis","text","Normal")],
    "Emergency":        [("CBC","g/dL","13.5-17.5"),
                         ("CMP","mg/dL","70-100"),
                         ("Troponin","ng/mL","0.0-0.4"),
                         ("Lactate","mmol/L","0.5-1.0")],
    "Gynecology":       [("Pap Smear","text","Normal"),
                         ("Beta HCG","mIU/mL","0-5"),
                         ("Pelvic Ultrasound","text","Normal")],
    "Psychiatry":       [("Thyroid Panel","mIU/L","0.4-4.0"),
                         ("Lithium Level","mEq/L","0.6-1.2"),
                         ("CBC","g/dL","13.5-17.5")],
    "Dermatology":      [("Skin Biopsy","text","Benign"),
                         ("Patch Test","text","Negative"),
                         ("ANA","titer","Negative")],
    "Nephrology":       [("Creatinine","mg/dL","0.6-1.2"),
                         ("eGFR","mL/min","60-120"),
                         ("Urine Protein","mg/dL","0-30")],
    "Gastroenterology": [("Colonoscopy","text","Normal"),
                         ("H. Pylori","text","Negative"),
                         ("Liver Function","U/L","7-56")],
    "Pulmonology":      [("Spirometry","L","3.5-5.0"),
                         ("Chest X-Ray","text","Normal"),
                         ("ABG","mmHg","75-100")],
    "Endocrinology":    [("TSH","mIU/L","0.4-4.0"),
                         ("HbA1c","%","4.0-5.6"),
                         ("Cortisol","μg/dL","6-23")],
}

# Helper: random data between two dates
def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def random_date_str(start, end):
    return random_date(start, end).strftime('%Y-%m-%d')

# GENERATOR 1: PATIENTS (500,000 rows)
# CONCEPT: SCD Type 2 — some patients change insurance over time
# We generate an effective_date and expiry_date for tracking
def generate_patients(n=NUM_PATIENTS):
    print(f"\n{'='*55}")
    print(f"  Generating {n:,} patients...")
    print(f"{'='*55}")
    t0 = time.time()

    states = list(STATES_CITIES.keys())
    rows = []

    for i in range(1, n + 1):
        state = random.choice(states)
        city  = random.choice(STATES_CITIES[state])
        dob   = random_date(datetime(1940,1,1), datetime(2010,12,31))
        eff   = random_date(START_DATE, datetime(2023,6,30))

        rows.append({
            "patient_id"      : f"P{i:07d}",
            "first_name"      : fake.first_name(),
            "last_name"       : fake.last_name(),
            "dob"             : dob.strftime('%Y-%m-%d'),
            "gender"          : random.choice(["M","F","M","F","O"]),
            "city"            : city,
            "state"           : state,
            "zip_code"        : fake.zipcode(),
            "insurance_type"  : random.choices(INSURANCE_TYPES, INSURANCE_WEIGHTS)[0],
            "blood_group"     : random.choice(["A+","A-","B+","B-","AB+","AB-","O+","O-"]),
            "phone"           : fake.phone_number(),
            "email"           : fake.email(),
            "effective_date"  : eff.strftime('%Y-%m-%d'),
            "expiry_date"     : "9999-12-31",   # SCD Type 2: current record
            "is_current"      : "Y",
            "created_at"      : eff.strftime('%Y-%m-%d %H:%M:%S'),
        })

        if i % 100_000 == 0:
            print(f"  ✓ {i:,} patients generated...")

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "patients.csv")
    df.to_csv(path, index=False)
    print(f"  ✅ patients.csv saved — {len(df):,} rows | {os.path.getsize(path)/1e6:.1f} MB | {time.time()-t0:.1f}s")
    return df

# GENERATOR 2: DEPARTMENTS (50 rows) & DOCTORS (10,000 rows)
def generate_departments():
    print(f"\n{'='*55}")
    print(f"  Generating departments...")
    rows = []
    for i, dept in enumerate(DEPARTMENTS, 1):
        rows.append({
            "department_id"  : f"DEPT{i:03d}",
            "department_name": dept,
            "floor"          : random.randint(1, 10),
            "building"       : random.choice(["Main","East Wing","West Wing","North Tower"]),
            "head_doctor_id" : f"D{random.randint(1,10000):07d}",
            "created_at"     : START_DATE.strftime('%Y-%m-%d'),
        })
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "departments.csv"), index=False)
    print(f"  ✅ departments.csv saved — {len(df):,} rows")
    return df

def generate_doctors(n=NUM_DOCTORS):
    print(f"\n{'='*55}")
    print(f"  Generating {n:,} doctors...")
    print(f"{'='*55}")
    t0 = time.time()
    rows = []
    specializations = DEPARTMENTS.copy()

    for i in range(1, n + 1):
        spec = random.choices(specializations, DEPT_WEIGHTS)[0]
        hire = random_date(datetime(2000,1,1), datetime(2023,1,1))
        exp  = (datetime.now() - hire).days // 365

        rows.append({
            "doctor_id"       : f"D{i:07d}",
            "first_name"      : fake.first_name(),
            "last_name"       : fake.last_name(),
            "specialization"  : spec,
            "department"      : spec,
            "qualification"   : random.choice(["MD","MBBS","DO","PhD"]),
            "hire_date"       : hire.strftime('%Y-%m-%d'),
            "experience_years": exp,
            "consultation_fee": round(random.uniform(100, 800), 2),
            "is_active"       : random.choices(["Y","N"], [0.95, 0.05])[0],
            "created_at"      : hire.strftime('%Y-%m-%d %H:%M:%S'),
        })

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "doctors.csv")
    df.to_csv(path, index=False)
    print(f"  ✅ doctors.csv saved — {len(df):,} rows | {os.path.getsize(path)/1e6:.1f} MB | {time.time()-t0:.1f}s")
    return df

# GENERATOR 3: APPOINTMENTS (3,000,000 rows)
# CONCEPT: Vectorized generation using numpy — much faster
# than looping row by row for millions of records
def generate_appointments(patient_ids, doctor_ids, n=NUM_APPOINTMENTS):
    print(f"\n{'='*55}")
    print(f"  Generating {n:,} appointments...")
    print(f"  (Using vectorized numpy — this is why we don't use Excel)")
    print(f"{'='*55}")
    t0 = time.time()

    # Vectorized random selections — generate all at once in memory
    dept_choices = np.random.choice(
        DEPARTMENTS, size=n, p=DEPT_WEIGHTS
    )
    visit_date_offsets = np.random.randint(
        0, (END_DATE - START_DATE).days, size=n
    )
    visit_dates = [
        (START_DATE + timedelta(days=int(d))).strftime('%Y-%m-%d')
        for d in visit_date_offsets
    ]

    rows = []
    for i in range(n):
        dept     = dept_choices[i]
        diag     = random.choice(DIAGNOSES.get(dept, [("Z00","General Exam")]))
        pat_id   = random.choice(patient_ids)
        doc_id   = random.choice(doctor_ids)
        vtype    = random.choices(VISIT_TYPES, VISIT_WEIGHTS)[0]
        status   = random.choices(
            ["Completed","Cancelled","No-Show","Pending"],
            [0.82, 0.08, 0.05, 0.05]
        )[0]

        rows.append({
            "visit_id"        : f"V{i+1:09d}",
            "patient_id"      : pat_id,
            "doctor_id"       : doc_id,
            "department"      : dept,
            "visit_date"      : visit_dates[i],
            "visit_type"      : vtype,
            "diagnosis_code"  : diag[0],
            "diagnosis_desc"  : diag[1],
            "status"          : status,
            "duration_minutes": random.randint(10, 120),
            "follow_up_needed": random.choices(["Y","N"], [0.35, 0.65])[0],
            "created_at"      : visit_dates[i] + " 08:00:00",
        })

        if (i+1) % 500_000 == 0:
            print(f"  ✓ {i+1:,} appointments generated...")

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "appointments.csv")
    df.to_csv(path, index=False)
    print(f"  ✅ appointments.csv saved — {len(df):,} rows | {os.path.getsize(path)/1e6:.1f} MB | {time.time()-t0:.1f}s")
    return df


# GENERATOR 4: LAB RESULTS (5,000,000 rows)
def generate_lab_results(visit_ids, patient_ids, n=NUM_LABS):
    print(f"\n{'='*55}")
    print(f"  Generating {n:,} lab results...")
    print(f"{'='*55}")
    t0 = time.time()

    rows = []
    dept_list = list(LAB_TESTS.keys())

    for i in range(n):
        dept   = random.choice(dept_list)
        test   = random.choice(LAB_TESTS[dept])
        is_abn = random.choices(["Y","N"], [0.28, 0.72])[0]

        # Realistic result values
        if test[1] == "text":
            result_val = "Abnormal" if is_abn == "Y" else "Normal"
        else:
            try:
                lo, hi = [float(x) for x in test[2].split("-")]
                if is_abn == "Y":
                    # Push value outside normal range
                    result_val = round(random.uniform(hi * 1.1, hi * 2.0), 2)
                else:
                    result_val = round(random.uniform(lo, hi), 2)
            except:
                result_val = round(random.uniform(1, 100), 2)

        test_date = random_date_str(START_DATE, END_DATE)

        rows.append({
            "lab_id"        : f"L{i+1:09d}",
            "visit_id"      : random.choice(visit_ids),
            "patient_id"    : random.choice(patient_ids),
            "department"    : dept,
            "test_name"     : test[0],
            "test_date"     : test_date,
            "result_value"  : str(result_val),
            "unit"          : test[1],
            "normal_range"  : test[2],
            "is_abnormal"   : is_abn,
            "reviewed_by"   : f"D{random.randint(1,10000):07d}",
            "created_at"    : test_date + " 10:00:00",
        })

        if (i+1) % 1_000_000 == 0:
            print(f"  ✓ {i+1:,} lab results generated...")

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "lab_results.csv")
    df.to_csv(path, index=False)
    print(f"  ✅ lab_results.csv saved — {len(df):,} rows | {os.path.getsize(path)/1e6:.1f} MB | {time.time()-t0:.1f}s")
    return df

# GENERATOR 5: BILLING (3,000,000 rows)
def generate_billing(visit_ids, patient_ids, n=NUM_BILLING):
    print(f"\n{'='*55}")
    print(f"  Generating {n:,} billing records...")
    print(f"{'='*55}")
    t0 = time.time()

    # Billing amounts by visit type — realistic ranges
    billing_ranges = {
        "OPD"       : (500,   5_000),
        "IPD"       : (8_000, 80_000),
        "Emergency" : (3_000, 40_000),
        "Telehealth": (200,   800),
    }

    rows = []
    for i in range(n):
        vtype        = random.choices(VISIT_TYPES, VISIT_WEIGHTS)[0]
        lo, hi       = billing_ranges[vtype]
        total        = round(random.uniform(lo, hi), 2)
        ins_type     = random.choices(INSURANCE_TYPES, INSURANCE_WEIGHTS)[0]
        coverage_pct = {"Private":0.80,"Medicare":0.75,"Medicaid":0.90,
                        "Government":0.85,"Uninsured":0.0}[ins_type]
        ins_covered  = round(total * coverage_pct, 2)
        patient_due  = round(total - ins_covered, 2)
        bill_date    = random_date_str(START_DATE, END_DATE)

        rows.append({
            "billing_id"       : f"B{i+1:09d}",
            "visit_id"         : random.choice(visit_ids),
            "patient_id"       : random.choice(patient_ids),
            "billing_date"     : bill_date,
            "visit_type"       : vtype,
            "insurance_type"   : ins_type,
            "total_amount"     : total,
            "insurance_covered": ins_covered,
            "patient_due"      : patient_due,
            "payment_status"   : random.choices(
                ["Paid","Pending","Overdue","Waived"],
                [0.65, 0.20, 0.10, 0.05]
            )[0],
            "payment_method"   : random.choice(
                ["Credit Card","Insurance","Cash","Check","Online"]
            ),
            "created_at"       : bill_date + " 12:00:00",
        })

        if (i+1) % 500_000 == 0:
            print(f"  ✓ {i+1:,} billing records generated...")

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "billing.csv")
    df.to_csv(path, index=False)
    print(f"  ✅ billing.csv saved — {len(df):,} rows | {os.path.getsize(path)/1e6:.1f} MB | {time.time()-t0:.1f}s")
    return df


# MAIN — Run all generators in sequence
if __name__ == "__main__":
    total_start = time.time()

    print("\n" + "="*55)
    print("  HealthFlow 360 — Data Generation Starting")
    print("  Target: ~11.5 Million rows across 6 files")
    print("="*55)

    # Step 1: Generate master dimension data first
    dept_df    = generate_departments()
    patient_df = generate_patients()
    doctor_df  = generate_doctors()

    # Step 2: Extract IDs for foreign key references
    patient_ids = patient_df["patient_id"].tolist()
    doctor_ids  = doctor_df["doctor_id"].tolist()

    # Step 3: Generate fact/transactional data
    appt_df    = generate_appointments(patient_ids, doctor_ids)
    visit_ids  = appt_df["visit_id"].tolist()

    lab_df     = generate_lab_results(visit_ids, patient_ids)
    billing_df = generate_billing(visit_ids, patient_ids)

    # Summary
    total_rows = (len(patient_df) + len(doctor_df) + len(dept_df) +
                  len(appt_df) + len(lab_df) + len(billing_df))

    print(f"\n{'='*55}")
    print(f"  🎉 ALL DATA GENERATED SUCCESSFULLY")
    print(f"  Total rows : {total_rows:,}")
    print(f"  Total time : {time.time()-total_start:.1f} seconds")
    print(f"  Location   : {OUTPUT_DIR}")
    print(f"{'='*55}\n")