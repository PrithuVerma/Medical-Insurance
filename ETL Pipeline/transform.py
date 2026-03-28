import pandas as pd
import numpy as np
import os
from datetime import date, timedelta
import config as con


def load_parquet(filename):
    path = con.RAW_DIR + filename
    df = pd.read_parquet(path)
    print(f"  Loaded {filename}: {len(df):,} rows")
    return df

def save_parquet(df, filename):
    path = con.PROCESSED_DIR + filename
    df.to_parquet(path, index=False)
    print(f"  Saved  {filename}: {len(df):,} rows")


# 1. Transform Patients
def transform_patients():
    df = load_parquet('patients.parquet')
    before = len(df)

    # Data types
    df['dob']       = pd.to_datetime(df['dob'])
    df['phone_num'] = df['phone_num'].astype(str)

    # Drop duplicates
    df.drop_duplicates(subset='patient_id', inplace=True)

    # Drop nulls in critical columns
    df.dropna(subset=['patient_id', 'first_name', 'last_name', 'dob', 'gender'], inplace=True)

    # Validate gender values
    df = df[df['gender'].isin(con.GENDERS)]

    after = len(df)
    print(f"  Patients dropped: {before - after:,} rows")
    save_parquet(df, 'patients.parquet')
    return df


# 2. Transform Hospitals
def transform_hospitals():
    df = load_parquet('hospitals.parquet')
    before = len(df)

    # Data types
    df['phone_num'] = df['phone_num'].astype(str)

    # Drop duplicates
    df.drop_duplicates(subset='hospital_id', inplace=True)

    # Drop nulls in critical columns
    df.dropna(subset=['hospital_id', 'name', 'city', 'state', 'type'], inplace=True)

    # Validate ENUM values
    df = df[df['type'].isin(con.HOSPITAL_TYPES)]

    after = len(df)
    print(f"  Hospitals dropped: {before - after:,} rows")
    save_parquet(df, 'hospitals.parquet')
    return df


# 3. Transform Policies
def transform_policies(patients_df):
    df = load_parquet('policies.parquet')
    before = len(df)

    # Data types
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date']   = pd.to_datetime(df['end_date'])

    # Validate ENUM values
    df = df[df['policy_type'].isin(con.POLICY_TYPES)]
    df = df[df['status'].isin(con.POLICY_STATUS)]

    # Fix: end_date must always be after start_date
    invalid_dates = df['end_date'] <= df['start_date']
    df.loc[invalid_dates, 'end_date'] = df.loc[invalid_dates, 'start_date'] + timedelta(days=365)
    print(f"  Policies fixed (end_date): {invalid_dates.sum():,} rows")

    # Fix: expired policies must have end_date in the past
    today = pd.Timestamp(date.today())
    expired_future = (df['status'] == 'Expired') & (df['end_date'] >= today)
    df.loc[expired_future, 'end_date'] = today - timedelta(days=random_days())
    print(f"  Policies fixed (expired end_date): {expired_future.sum():,} rows")

    # Fix: premium should be 1-5% of coverage
    df['premium_amount'] = (
        df['coverage_amount'] * np.random.uniform(0.01, 0.05, size=len(df))
    ).round(2)

    # Validate FK — patient_id must exist in patients
    valid_patients = set(patients_df['patient_id'])
    before_fk = len(df)
    df = df[df['patient_id'].isin(valid_patients)]
    print(f"  Policies dropped (invalid patient_id FK): {before_fk - len(df):,} rows")

    # Drop nulls in critical columns
    df.dropna(subset=['policy_id', 'patient_id', 'coverage_amount', 'start_date', 'end_date'], inplace=True)

    after = len(df)
    print(f"  Policies dropped (total): {before - after:,} rows")
    save_parquet(df, 'policies.parquet')
    return df


# 4. Transform Claims
def transform_claims(policies_df, hospitals_df):
    df = load_parquet('claims.parquet')
    before = len(df)

    # Data types
    df['claim_date'] = pd.to_datetime(df['claim_date'])

    # Validate ENUM values
    df = df[df['status'].isin(con.CLAIM_STATUS)]

    # Validate FKs
    valid_policies  = set(policies_df['policy_id'])
    valid_hospitals = set(hospitals_df['hospital_id'])
    df = df[df['policy_id'].isin(valid_policies)]
    df = df[df['hospital_id'].isin(valid_hospitals)]

    # Merge policy dates to validate claim_date range
    policy_dates = policies_df[['policy_id', 'start_date', 'end_date', 'coverage_amount']]
    df = df.merge(policy_dates, on='policy_id', how='left')

    # Drop: claim_date outside policy start/end — fundamentally contradictory
    valid_date_range = (
        (df['claim_date'] >= df['start_date']) &
        (df['claim_date'] <= df['end_date'])
    )
    dropped_dates = (~valid_date_range).sum()
    df = df[valid_date_range]
    print(f"  Claims dropped (claim_date out of policy range): {dropped_dates:,} rows")

    # Fix: approved_amount must not exceed coverage_amount
    over_coverage = df['approved_amount'] > df['coverage_amount']
    df.loc[over_coverage, 'approved_amount'] = (
        df.loc[over_coverage, 'coverage_amount'] * np.random.uniform(0.5, 1.0, size=over_coverage.sum())
    ).round(2)
    print(f"  Claims fixed (approved_amount capped): {over_coverage.sum():,} rows")

    # Drop merge columns — not needed in final table
    df.drop(columns=['start_date', 'end_date', 'coverage_amount'], inplace=True)

    # Drop nulls
    df.dropna(subset=['claim_id', 'policy_id', 'hospital_id', 'claim_date', 'total_amount'], inplace=True)

    after = len(df)
    print(f"  Claims dropped (total): {before - after:,} rows")
    save_parquet(df, 'claims.parquet')
    return df


# 5. Transform Procedures
def transform_procedures(claims_df):
    df = load_parquet('procedures.parquet')
    before = len(df)

    # Data types
    df['procedure_date'] = pd.to_datetime(df['procedure_date'])

    # Validate ENUM values
    df = df[df['category'].isin(con.PROCEDURE_CATS)]

    # Validate FK
    valid_claims = set(claims_df['claim_id'])
    df = df[df['claim_id'].isin(valid_claims)]

    # Merge claim_date to validate procedure_date
    claim_dates = claims_df[['claim_id', 'claim_date']]
    df = df.merge(claim_dates, on='claim_id', how='left')

    # Fix: procedure_date must not be before claim_date
    before_claim = df['procedure_date'] < df['claim_date']
    df.loc[before_claim, 'procedure_date'] = df.loc[before_claim, 'claim_date']
    print(f"  Procedures fixed (procedure_date): {before_claim.sum():,} rows")

    # Drop merge column
    df.drop(columns=['claim_date'], inplace=True)

    # Drop nulls
    df.dropna(subset=['procedure_id', 'claim_id', 'procedure_name', 'cost', 'procedure_date'], inplace=True)

    after = len(df)
    print(f"  Procedures dropped (total): {before - after:,} rows")
    save_parquet(df, 'procedures.parquet')
    return df


# 6. Transform Claim Reviews
def transform_claim_reviews(claims_df):
    df = load_parquet('claim_reviews.parquet')
    before = len(df)

    # Data types
    df['review_date'] = pd.to_datetime(df['review_date'])

    # Validate ENUM values
    df = df[df['decision'].isin(con.DECISIONS)]

    # Validate FK
    valid_claims = set(claims_df['claim_id'])
    df = df[df['claim_id'].isin(valid_claims)]

    # Merge claim_date to validate review_date
    claim_dates = claims_df[['claim_id', 'claim_date']]
    df = df.merge(claim_dates, on='claim_id', how='left')

    # Fix: review_date must be after claim_date
    before_claim = df['review_date'] < df['claim_date']
    df.loc[before_claim, 'review_date'] = (
        df.loc[before_claim, 'claim_date'] + timedelta(days=1)
    )
    print(f"  Claim Reviews fixed (review_date): {before_claim.sum():,} rows")

    # Fix: approved decisions should not have rejection_reason
    df.loc[df['decision'] == 'Approved', 'rejection_reason'] = None

    # Drop merge column
    df.drop(columns=['claim_date'], inplace=True)

    # Drop nulls in critical columns
    df.dropna(subset=['review_id', 'claim_id', 'review_date', 'decision'], inplace=True)

    after = len(df)
    print(f"  Claim Reviews dropped (total): {before - after:,} rows")
    save_parquet(df, 'claim_reviews.parquet')
    return df


def random_days():
    return np.random.randint(30, 365)


# Master Transform Function
def transform():
    print("\n===== TRANSFORM =====")
    os.makedirs(con.PROCESSED_DIR, exist_ok=True)

    patients  = transform_patients()
    hospitals = transform_hospitals()
    policies  = transform_policies(patients)
    claims    = transform_claims(policies, hospitals)
    procedures    = transform_procedures(claims)
    claim_reviews = transform_claim_reviews(claims)

    print("\n✓ Transform complete — all Parquet files saved to data/processed/\n")
    return patients, hospitals, policies, claims, procedures, claim_reviews


if __name__ == "__main__":
    transform()