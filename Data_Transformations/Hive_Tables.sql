-- Databricks notebook source
-- MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_e2e;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.customer_details_external
(
  customer_key STRING,
  ingest_date TIMESTAMP,
  customer_id STRING,
  member_id STRING,
  first_name STRING,
  last_name STRING,
  permanent_status STRING,
  age INT,
  state STRING,
  country STRING
)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/customer_details"

-- COMMAND ----------

select count(*) from lending_loan_e2e.customer_details_external

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.loan_details_external
(
  loan_key STRING,
  ingest_date TIMESTAMP,
  loan_id STRING,
  member_id STRING,
  account_id STRING,
  loan_amount DOUBLE,
  funded_amount DOUBLE,
  term INT,
  interest FLOAT,
  installment FLOAT,
  issue_date DATE,
  loan_status STRING,
  purpose STRING,
  title STRING,
  disbursement_method STRING
)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/loan_details"

-- COMMAND ----------

select count(*) from lending_loan_e2e.loan_details_external

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS lending_loan_e2e.account_details_external
(
  account_key STRING,
  ingest_date TIMESTAMP,
  account_id STRING,
  member_id STRING,
  loan_id STRING,
  grade STRING,
  sub_grade STRING,
  employee_designation STRING,
  employee_experience INT,
  home_ownership STRING,
  annual_income FLOAT,
  verification_status STRING,
  total_high_credit_limit FLOAT,
  application_type STRING,
  annual_income_joint FLOAT,
  verification_status_joint STRING
)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/account_details"

-- COMMAND ----------

select count(*) from lending_loan_e2e.account_details_external

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lending_loan_e2e.payment_details_external
(
  payments_key STRING,
  ingest_date TIMESTAMP,
  loan_id STRING,
  member_id STRING,
  latest_transaction STRING,
  funded_amount_investor DOUBLE,
  total_payment_recorded FLOAT,
  installment FLOAT,
  last_payment_amount FLOAT,
  last_payment_date DATE,
  next_payment_date DATE,
  payment_method STRING

)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/loan_payment"

-- COMMAND ----------

select count(*) from lending_loan_e2e.payment_details_external

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lending_loan_e2e.loanDefaulters_details_external
(
  loan_default_key STRING,
  ingest_date TIMESTAMP,
  loan_id STRING,
  member_id STRING,
  loan_defaulter_id STRING,
  defaulters_2yrs FLOAT,
  defaulters_amount DOUBLE,
  public_records INT,
  public_record_bankruptcies INT,
  enquiries_6mnths INT,
  late_fee FLOAT,
  hardship_flag STRING,
  hardship_type STRING,
  hardship_length INT,
  hardship_amount FLOAT
)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/loan-defaulters"

-- COMMAND ----------

select count(*) from lending_loan_e2e.loanDefaulters_details_external

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lending_loan_e2e.investors_details_details
(
  investors_key STRING,
  ingest_date TIMESTAMP,
  investor_loan_id STRING,
  loan_id STRING,
  investor_id STRING,
  loan_fund_amnt FLOAT,
  investor_type STRING,
  age INT,
  state STRING,
  country STRING
)
USING PARQUET
LOCATION "/mnt/kushagrafinance/cleaned-data/Lending_Loan/loan_investors"