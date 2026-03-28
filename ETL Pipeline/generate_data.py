import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import date, timedelta

class con:
    DB_HOST     = "localhost"
    DB_PORT     = 3306
    DB_USER     = "root"
    DB_PASSWORD = ""
    DB_NAME     = "medical_insurance"

    RAW_DIR       = "data/raw/"
    PROCESSED_DIR = "data/processed/"

    NUM_PATIENTS      = 750_000
    NUM_HOSPITALS     = 130
    NUM_POLICIES      = 300_000
    NUM_CLAIMS        = 300_000
    NUM_PROCEDURES    = 750_000
    NUM_CLAIM_REVIEWS = 50_000

    GENDERS        = ["Male", "Female", "Other"]
    POLICY_TYPES   = ["Health", "Dental", "Vision", "Critical Illness"]
    POLICY_STATUS  = ["Active", "Expired", "Cancelled"]
    CLAIM_STATUS   = ["Pending", "Approved", "Denied"]
    HOSPITAL_TYPES = ["Public", "Private", "Clinic"]
    PROCEDURE_CATS = ["Surgery", "Consultation", "Diagnostic", "Therapy"]
    DECISIONS      = ["Approved", "Denied", "Escalated"]

fake = Faker('en_IN')
os.makedirs(con.RAW_DIR, exist_ok=True)
os.makedirs(con.PROCESSED_DIR, exist_ok=True)

def generate_phone():
    first_digit = random.choice(['6', '7', '8', '9'])
    rest = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    return first_digit + rest


# Patients 
def generate_patients():
    print("Generating patients...")
    patients = []
    for i in range(1, con.NUM_PATIENTS + 1):
        dob = fake.date_between(start_date=date(1945, 1, 1), end_date=date(2007, 1, 1))
        patients.append({
            'patient_id' : f'PAT{i:07d}',
            'first_name' : fake.first_name(),
            'last_name'  : fake.last_name(),
            'dob'        : dob,
            'gender'     : random.choice(con.GENDERS),
            'phone_num'  : generate_phone(),
            'address'    : fake.address(),
            'email'      : fake.free_email()
        })

    df = pd.DataFrame(patients)
    df.to_parquet(con.RAW_DIR + 'patients.parquet', index=False)
    print(f"  ✓ Patients: {len(df):,} rows")
    return df


# Hospitals
def generate_hospitals():
    print("Generating hospitals...")
    hospitals = []
    for i in range(1, con.NUM_HOSPITALS + 1):
        hospitals.append({
            'hospital_id' : f'HSP{i:04d}',
            'name'        : fake.company() + " Hospital",
            'city'        : fake.city(),
            'state'       : fake.state(),
            'type'        : random.choice(con.HOSPITAL_TYPES),
            'phone_num'   : generate_phone(),
            'email'       : fake.company_email()
        })

    df = pd.DataFrame(hospitals)
    df.to_parquet(con.RAW_DIR + 'hospitals.parquet', index=False)
    print(f"  ✓ Hospitals: {len(df):,} rows")
    return df


# Policies 
def generate_policies(patient_ids):
    print("Generating policies...")

    # Estimating only 40% of patients have insurance
    insured_patients = np.random.choice(patient_ids, size=con.NUM_POLICIES, replace=False)

    # Coverage amounts vary by policy type — right skewed (most are low, few are high)
    coverage_amounts = {
        "Health"          : (200_000,  1_000_000),
        "Dental"          : (50_000,   200_000),
        "Vision"          : (20_000,   100_000),
        "Critical Illness": (500_000,  5_000_000)
    }

    policies = []
    for i, pid in enumerate(insured_patients, 1):
        policy_type     = random.choice(con.POLICY_TYPES)
        low, high       = coverage_amounts[policy_type]
        coverage_amount = round(np.random.uniform(low, high), 2)
        premium_amount  = round(coverage_amount * np.random.uniform(0.01, 0.05), 2)
        start_date      = fake.date_between(start_date=date(2015, 1, 1), end_date=date(2023, 1, 1))
        end_date        = start_date + timedelta(days=random.choice([365, 730, 1095]))  # 1, 2, or 3 years

        policies.append({
            'policy_id'         : f'POL{i:07d}',
            'patient_id'        : pid,
            'policy_type'       : policy_type,
            'insurance_company' : fake.company() + " Insurance",
            'status'            : random.choice(con.POLICY_STATUS),
            'coverage_amount'   : coverage_amount,
            'premium_amount'    : premium_amount,
            'start_date'        : start_date,
            'end_date'          : end_date
        })

    df = pd.DataFrame(policies)
    df.to_parquet(con.RAW_DIR + 'policies.parquet', index=False)
    print(f"  ✓ Policies: {len(df):,} rows")
    return df


# Claims
def generate_claims(policy_ids, hospital_ids):
    print("Generating claims...")

    # Skewed claim amounts — most claims are small, few are large (realistic)
    claims = []
    for i in range(1, con.NUM_CLAIMS + 1):
        total_amount    = round(np.random.exponential(scale=50_000), 2)  # exponential = right skewed
        approved_amount = round(total_amount * np.random.uniform(0.5, 1.0), 2)
        claim_date      = fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 12, 31))

        claims.append({
            'claim_id'        : f'CLM{i:07d}',
            'policy_id'       : random.choice(policy_ids),
            'hospital_id'     : random.choice(hospital_ids),
            'claim_date'      : claim_date,
            'total_amount'    : total_amount,
            'approved_amount' : approved_amount,
            'status'          : random.choice(con.CLAIM_STATUS)
        })

    df = pd.DataFrame(claims)
    df.to_parquet(con.RAW_DIR + 'claims.parquet', index=False)
    print(f"  ✓ Claims: {len(df):,} rows")
    return df


# Procedures
PROCEDURE_NAMES = {
    "Surgery"      : ["Appendectomy", "Knee Replacement", "Cataract Surgery", "Bypass Surgery", "Gallbladder Removal"],
    "Consultation" : ["General Checkup", "Specialist Consultation", "Follow-up Visit", "Emergency Consultation"],
    "Diagnostic"   : ["MRI Scan", "CT Scan", "Blood Test", "X-Ray", "ECG", "Ultrasound"],
    "Therapy"      : ["Physiotherapy", "Chemotherapy", "Dialysis", "Radiation Therapy", "Occupational Therapy"]
}

def generate_procedures(claim_ids):
    print("Generating procedures...")
    procedures = []
    i = 1
    for claim_id in claim_ids:
        # Each claim has 1-4 procedures
        num_procedures = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
        claim_date     = fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 12, 31))

        for _ in range(num_procedures):
            category = random.choice(con.PROCEDURE_CATS)
            procedures.append({
                'procedure_id'   : f'PRC{i:07d}',
                'claim_id'       : claim_id,
                'procedure_name' : random.choice(PROCEDURE_NAMES[category]),
                'category'       : category,
                'cost'           : round(np.random.uniform(500, 150_000), 2),
                'procedure_date' : claim_date + timedelta(days=random.randint(0, 7))
            })
            i += 1

    df = pd.DataFrame(procedures)
    df.to_parquet(con.RAW_DIR + 'procedures.parquet', index=False)
    print(f"  ✓ Procedures: {len(df):,} rows")
    return df


# Claim Reviews
REJECTION_REASONS = [
    "Pre-existing condition not covered",
    "Policy lapsed at time of claim",
    "Procedure not covered under plan",
    "Insufficient documentation",
    "Duplicate claim",
    "Outside network hospital",
    None  # Most reviews don't have a rejection reason (approved)
]

def generate_claim_reviews(claim_ids):
    print("Generating claim reviews...")

    # Only a subset of claims get reviewed
    reviewed_claims = np.random.choice(claim_ids, size=con.NUM_CLAIM_REVIEWS, replace=False)

    reviews = []
    for i, claim_id in enumerate(reviewed_claims, 1):
        decision = random.choice(con.DECISIONS)
        reviews.append({
            'review_id'        : f'REV{i:07d}',
            'claim_id'         : claim_id,
            'review_date'      : fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 12, 31)),
            'decision'         : decision,
            'reviewer_name'    : fake.name(),
            'rejection_reason' : None if decision == "Approved" else random.choice(REJECTION_REASONS[:-1])
        })

    df = pd.DataFrame(reviews)
    df.to_parquet(con.RAW_DIR + 'claim_reviews.parquet', index=False)
    print(f"  ✓ Claim Reviews: {len(df):,} rows")
    return df


# Master Extract Function
def extract():
    print("\n===== EXTRACT =====")
    patients  = generate_patients()
    hospitals = generate_hospitals()
    policies  = generate_policies(patients['patient_id'].tolist())
    claims    = generate_claims(policies['policy_id'].tolist(), hospitals['hospital_id'].tolist())
    procedures    = generate_procedures(claims['claim_id'].tolist())
    claim_reviews = generate_claim_reviews(claims['claim_id'].tolist())
    print("\n✓ Extract complete — all Parquet files saved to data/raw/\n")
    return patients, hospitals, policies, claims, procedures, claim_reviews


if __name__ == "__main__":
    extract()