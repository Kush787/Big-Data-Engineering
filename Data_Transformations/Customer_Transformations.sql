-- Databricks notebook source
SELECT state,country,count(*) as total_count
FROM lending_loan_e2e.customer_details_external
GROUP BY state,country;

-- COMMAND ----------

SELECT country, 
       CASE 
           WHEN age BETWEEN 18 AND 25 THEN 'Youngsters'
           WHEN age BETWEEN 26 AND 35 THEN 'Working class'
           WHEN age BETWEEN 36 AND 45 THEN 'Middle Age'
           ELSE 'Senior Citizens'
       END as age_range,
       COUNT(*)
 FROM lending_loan_e2e.customer_details_external
WHERE permanent_status = 'TRUE'
GROUP BY country, age_range;



-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.customer_premium_details
USING PARQUET
LOCATION "/mnt/kushagrafinance/processed-data/lending_loan/customer_transformations/customers_premium_status"

WITH customer_counts AS 
(
  SELECT country, state,count(*) as total_customers
  FROM lending_loan_e2e.customer_details_external
  GROUP BY country,state
),
member_counts AS 
(
  SELECT country, state, COUNT(DISTINCT member_id) as total_members
  FROM lending_loan_e2e.customer_details_external
  WHERE member_id IS NOT NULL and permanent_status ='TRUE'
  GROUP BY country,state
)
SELECT  customer_counts.country, customer_counts.state,round(member_counts.total_members / customer_counts.total_customers * 100,2) as member_percentage
FROM customer_counts
JOIN member_counts
ON customer_counts.country = member_counts.country AND customer_counts.state = member_counts.state;

-- COMMAND ----------

