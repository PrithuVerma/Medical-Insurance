# 🏥 Healthcare Insurance Claims Analytics

## Overview

This project simulates how an insurance company processes healthcare claims — from patient registration all the way to claim approval or rejection.

Instead of just building tables and running queries, the goal here was simple:
**think like a real analyst inside an insurance company.**

---

## The Idea

Insurance claims data is messy, interconnected, and full of hidden patterns.

So I built a system that answers questions like:

* Which hospitals have unusually high claim costs?
* Where are most claims getting denied — and why?
* Are certain patients or procedures driving costs up?
* Can we spot suspicious (potentially fraudulent) claims?

---

## Tech Stack

* **SQL** → MySQL
* **Python** → Pandas, NumPy
* **Data Generation** → Faker
* **ETL** → SQLAlchemy
* **Visualization (optional)** → Matplotlib / Power BI

---

## Schema
<img width="949" height="600" alt="Screenshot 2026-03-28 at 5 23 09 PM" src="https://github.com/user-attachments/assets/193f4f78-c13b-44d9-9968-5b0cdc7f4f9f" />


## What This Project Covers

### 📊 SQL (Core of the project)

This is where most of the heavy lifting happens.

* **Complex Joins**
  Connecting patients → policies → claims → hospitals → procedures
  (basically reconstructing the full claim journey)

* **CTEs (Common Table Expressions)**
  Used to model the lifecycle of a claim:

  ```
  Submitted → Reviewed → Approved/Denied
  ```

* **Window Functions**

  * Claim cost percentiles per hospital
  * Denial rate trends over time
  * Ranking high-cost claims

* **Aggregations & Insights**

  * Average claim cost by hospital
  * Denial rates by provider
  * Cost distribution across procedures

---

### 🐍 Python (Analysis Layer)

After extracting data from SQL, Python is used to actually **understand the story**.

* **EDA (Exploratory Data Analysis)**

  * Distribution of claim amounts
  * Most common rejection reasons
  * Outlier hospitals/providers

* **Cohort Analysis**

  * Patients with repeated claims
  * High-frequency vs low-frequency claimants

* **NumPy / Stats**

  * Cost normalization
  * Basic anomaly detection
  * Realistic distribution throughout the data tables

* **ETL Pipeline**

  * Data generated using Faker
  * Loaded into MySQL using SQLAlchemy
  * Queried and analyzed in Python

---

## Dataset

* Fully **synthetic dataset** generated using Faker
* Designed to mimic realistic claim patterns
* Exported and managed via MySQL Workbench

---

## Key Insights (Examples)

Some of the types of insights this system can produce:

* A small number of hospitals account for a large share of total claim costs
* Certain procedures consistently lead to higher rejection rates
* Repeated claims from the same patient cluster around specific conditions
* Outliers in claim amounts can indicate potential fraud

---

