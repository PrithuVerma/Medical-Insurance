CREATE DATABASE medical_insurance;
use medical_insurance;

SELECT 'patients' AS table_name, COUNT(*) AS row_count FROM patients
UNION ALL
SELECT 'hospitals', COUNT(*) FROM hospitals
UNION ALL
SELECT 'policies', COUNT(*) FROM policies
UNION ALL
SELECT 'claims', COUNT(*) FROM claims
UNION ALL
SELECT 'procedures', COUNT(*) FROM procedures
UNION ALL
SELECT 'claim_reviews', COUNT(*) FROM claim_reviews;

# ========= ADDING PRIMARY KEYS ========= #

alter table patients modify patient_id VARCHAR(10);
alter table patients add primary key (patient_id);
describe patients;

alter table claims modify claim_id varchar(15);
alter table claims add primary key (claim_id);
describe claims;

alter table claim_reviews modify review_id varchar(15);
alter table claim_reviews add primary key (review_id);
describe claim_reviews;

alter table hospitals modify hospital_id varchar(15);
alter table hospitals add primary key (hospital_id);
describe hospitals;

alter table policies modify policy_id varchar(15);
alter table policies add primary key (policy_id);
describe policies;

alter table procedures modify procedure_id varchar(15);
alter table procedures add primary key (procedure_id);
describe procedures;

# ========== ADDING FOREIGN KEYS ========= #

alter table procedures modify claim_id varchar(15);
alter table procedures 
	add constraint fk_pro_claim
	foreign key(claim_id) references claims(claim_id);
describe procedures;

alter table claims modify policy_id varchar(15);
alter table claims modify hospital_id varchar(15);
alter table claims 
	add constraint fk_cl_policy 
    foreign key(policy_id) references policies(policy_id);
alter table claims 
	add constraint fk_cl_hospital 
    foreign key(hospital_id) references hospitals(hospital_id);
describe claims;

alter table claim_reviews modify claim_id varchar(15);
	alter table claim_reviews add constraint fk_cl_claim 
    foreign key(claim_id) references claims(claim_id);
describe claim_reviews;

alter table policies modify patient_id varchar(15);
	alter table policies add constraint fk_pol_patient
    foreign key(patient_id) references patients(patient_id);
describe policies



