import pandas as pd
import config as con
import sqlalchemy as sql

connector = sql.create_engine(f'mysql+pymysql://{con.DB_USER}:{con.DB_PASSWORD}@{con.DB_HOST}:{con.DB_PORT}/{con.DB_NAME}')

def load_patients():
    print("Loading patients...")
    df_pat = pd.read_parquet(con.PROCESSED_DIR + 'patients.parquet')
    df_pat.to_sql('Patients',connector,if_exists= 'replace',index = False)
    return df_pat

def load_hospitals():
    print("Loading hospitals...")
    df_hos = pd.read_parquet(con.PROCESSED_DIR + 'hospitals.parquet')
    df_hos.to_sql('Hospitals',connector,if_exists= 'replace',index = False)
    return df_hos

def load_policies():
    print("Loading policies...")
    df_pol = pd.read_parquet(con.PROCESSED_DIR + 'policies.parquet')
    df_pol.to_sql('Policies', connector,if_exists='replace',index = False)
    return df_pol

def load_claims():
    print("Loading claims...")
    df_cla = pd.read_parquet(con.PROCESSED_DIR + 'claims.parquet')
    df_cla.to_sql('Claims', connector,if_exists='replace',index = False)
    return df_cla

def load_procedures():
    print("Loading procedures...")
    df_pro = pd.read_parquet(con.PROCESSED_DIR + 'procedures.parquet')
    df_pro.to_sql('Procedures',connector,if_exists='replace',index = False)
    return df_pro

def load_claim_reviews():
    print("Loading claim reviews...")
    df_cal_rev = pd.read_parquet(con.PROCESSED_DIR + 'claim_reviews.parquet')
    df_cal_rev.to_sql('Claim_Reviews',connector,if_exists='replace',index = False)
    return df_cal_rev
    
def loader():
    print('Master Loader Working ....\n')
    patients = load_patients()
    hospitals = load_hospitals()
    policies = load_policies()
    claims  = load_claims()
    procedures = load_procedures()
    claim_reviews = load_claim_reviews()
    
    print("\n✓ Loading complete — all Parquet files loaded to MySQL Workbench")
    return patients, hospitals, policies, claims, procedures, claim_reviews

if __name__ == '__main__':
    loader()