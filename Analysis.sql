use medical_insurance;

# ======= Claim Status =======#
select 
count(case when claims.status = 'Pending' then 1 end) as Pending,
count(case when claims.status = 'Approved' then 1 end) as Approved,
count(case when claims.status = 'Denied' then 1 end) as Denied
from claims;

select sum(status = 'Pending') as pending,
sum(status = 'Approved') as approved,
sum(status = 'Denied') as denied 
from claims;

				# ===== OR ===== #

select status, COUNT(*) AS total
from claims
group by status;

# How many claims were approved ? What is the total approved amount vs total coverage amount?

select policies.policy_id,
round(sum(claims.approved_amount),2) as Amount_Approved,
round(max(policies.coverage_amount),2) as Coverage_Amount
from Policies
left join claims on claims.policy_id = policies.policy_id
where claims.status = 'Approved'
group by policies.policy_id
order by Amount_Approved desc;

# Which procedure categories contribute most to claims and which ones have the highest approval rate?

select procedures.category as Category,
COUNT(CASE WHEN claims.status = 'Approved' THEN 1 END) / COUNT(*) * 100 as Approval_Rate,
round(sum(claims.approved_amount),2) as Approved_Amt
from claims
left join procedures on Procedures.claim_id = claims.claim_id
group by Category
order by Approved_Amt desc;

# Which hospitals have the highest claims provided, broken down by hospital type?

with Hospital_Claims as (
	select hospitals.name as Hospital,
    hospitals.type as Type,
	count(claims.claim_id) as Claims_Provided
	from claims 
	left join hospitals on hospitals.hospital_id = claims.hospital_id
	group by Hospital, Type
    )

select Hospital_Claims.Hospital, Hospital_Claims.Type, Hospital_Claims.Claims_Provided,
	Rank() over(partition by Hospital_Claims.type order by Hospital_Claims.Claims_Provided desc) as Ranking
from Hospital_Claims;

# Monthly and quarterly P&L — how much premium collected vs how much aid paid out, and what's the ratio

WITH Premium_Aid AS (
    SELECT
        YEAR(policies.start_date)        AS Year,
        QUARTER(policies.start_date)     AS Quarter,
        MONTH(policies.start_date)       AS Month,
        policies.policy_type             AS Policy_Type,
        ROUND(SUM(policies.premium_amount), 2)   AS Premium_Collected,
        ROUND(SUM(claims.approved_amount), 2)    AS Aid_Provided,
        ROUND(SUM(policies.premium_amount) - SUM(claims.approved_amount), 2) AS Net_PnL
    FROM claims
    LEFT JOIN policies ON policies.policy_id = claims.policy_id
    GROUP BY Year, Quarter, Month, Policy_Type
),
PnL_Status AS (
    SELECT
        Year,
        Quarter,
        Month,
        Policy_Type,
        Premium_Collected,
        Aid_Provided,
        Net_PnL,
        CASE
            WHEN Net_PnL > 0 THEN 'Profit'
            WHEN Net_PnL < 0 THEN 'Loss'
            ELSE 'Break Even'
        END AS PL_Status,
        ROUND(Premium_Collected / NULLIF(Aid_Provided, 0), 2) AS Premium_Aid_Ratio
    FROM Premium_Aid
)
SELECT *
FROM PnL_Status
ORDER BY Year, Quarter, Month, Policy_Type;

# Most common rejection reason

select rejection_reason, count(*) as total from claim_reviews
group by rejection_reason
order by total desc

