# Databricks notebook source
#calculate score for customers payment history

# COMMAND ----------

spark.conf.set("spark.sql.unacceptable_rated_pts",0)
spark.conf.set("spark.sql.very_bad_rated_pts",100)
spark.conf.set("spark.sql.bad_rated_pts",250)
spark.conf.set("spark.sql.good_rated_pts",500)
spark.conf.set("spark.sql.very_good_rated_pts",650)
spark.conf.set("spark.sql.excellent_rated_pts",800)


# COMMAND ----------

spark.conf.set("spark.sql.unacceptable_grade_pts",750)
spark.conf.set("spark.sql.very_bad_grade_pts",1000)
spark.conf.set("spark.sql.bad_grade_pts",1500)
spark.conf.set("spark.sql.good_grade_pts",2000)
spark.conf.set("spark.sql.very_good_grade_pts",2500)

# COMMAND ----------

unacceptable_grade = "F"
very_bad_grade = "E"
bad_grade = "D"
good_grades = "C"
very_good_grade = "B"
excellent_grade = "A"

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

account_df = spark.read.parquet(f"{cleaned_files_path}account_details")

# COMMAND ----------

loan_df = spark.read.parquet(f"{cleaned_files_path}loan_details")

# COMMAND ----------

payment_df = spark.read.parquet(f"{cleaned_files_path}loan_payment")

# COMMAND ----------

loan_def_df = spark.read.parquet(f"{cleaned_files_path}loan-defaulters")

# COMMAND ----------

customer_df = spark.read.parquet(f"{cleaned_files_path}customer_details")

# COMMAND ----------

customer_df.createOrReplaceTempView("customer_details")

# COMMAND ----------

payment_df.createOrReplaceTempView("payment_details")

# COMMAND ----------

payment_last_df = spark.sql("select c.member_id,c.state,c.country,c.first_name,c.last_name, \
    case \
        when p.last_payment_amount < (p.installment * 0.5) then ${spark.sql.very_bad_rated_pts} \
        when p.last_payment_amount >=(p.installment *0.5) and p.last_payment_amount<p.installment then ${spark.sql.bad_rated_pts} \
        when p.last_payment_amount = (p.installment) then ${spark.sql.good_rated_pts} \
        when p.last_payment_amount >(p.installment) and p.last_payment_amount <=(p.installment *1.50) then ${spark.sql.very_good_rated_pts} \
        when p.last_payment_amount > (p.installment *1.50) then ${spark.sql.excellent_rated_pts} \
        else ${spark.sql.unacceptable_rated_pts} \
    end as last_payment_pts, \
    case \
        when p.total_payment_recorded >=(p.funded_amount_investor * 0.50) then ${spark.sql.very_good_rated_pts} \
        when p.total_payment_recorded < (p.funded_amount_investor * 0.50) and p.total_payment_recorded > 0 then ${spark.sql.good_rated_pts} \
        when p.total_payment_recorded = 0 or (p.total_payment_recorded) is null then ${spark.sql.unacceptable_rated_pts} \
        end as total_payment_pts \
    from payment_details p inner join customer_details c on c.member_id=p.member_id")

# COMMAND ----------

payment_last_df.createOrReplaceTempView("payment_points_df")
spark.sql("select * from payment_points_df").show()

# COMMAND ----------

spark.sql("select * from payment_points_df where last_payment_pts!=500 or total_payment_pts!=500").show()

# COMMAND ----------

loan_def_df.createOrReplaceTempView("loan_default_details")
spark.sql("select * from loan_default_details").show()

# COMMAND ----------

loan_default_pts = spark.sql("select p.*, \
    case \
        when l.defaulters_2yrs = 0 then ${spark.sql.excellent_rated_pts} \
        when l.defaulters_2yrs between 1 and 2 then ${spark.sql.bad_rated_pts} \
        when l.defaulters_2yrs between 3 and 5 then ${spark.sql.very_bad_rated_pts} \
        when l.defaulters_2yrs > 5 OR l.defaulters_2yrs is null then ${spark.sql.unacceptable_rated_pts} \
    end as delinq_pts, \
    case \
        when l.public_records = 0 then ${spark.sql.excellent_rated_pts} \
        when l.public_records between 1 and 2 then ${spark.sql.bad_rated_pts} \
        when l.public_records between 3 and 5 then ${spark.sql.very_bad_rated_pts} \
        when l.public_records > 5 or l.public_records is null then ${spark.sql.very_bad_rated_pts} \
    end as public_records_pts, \
    case \
        when l.public_record_bankruptcies = 0 then ${spark.sql.excellent_rated_pts} \
        when l.public_record_bankruptcies between 1 and 2 then ${spark.sql.bad_rated_pts} \
        when l.public_record_bankruptcies between 3 and 5 then ${spark.sql.very_bad_rated_pts} \
        when l.public_record_bankruptcies > 5 or l.public_record_bankruptcies is null then ${spark.sql.unacceptable_rated_pts} \
    end as public_bankruptcies_pts, \
    case \
        when l.enquiries_6mnths = 0 then ${spark.sql.excellent_rated_pts} \
        when l.enquiries_6mnths between 1 and 2 then ${spark.sql.bad_rated_pts} \
        when l.enquiries_6mnths between 3 and 5 then ${spark.sql.very_bad_rated_pts} \
        when l.enquiries_6mnths > 5 or l.enquiries_6mnths is null then ${spark.sql.unacceptable_rated_pts} \
    end as enq_pts, \
    case \
        when l.hardship_flag = 'N' then ${spark.sql.very_good_rated_pts} \
        when l.hardship_flag = 'Y' or l.hardship_flag is null then ${spark.sql.bad_rated_pts} \
    end as hardship_pts \
    from loan_default_details l left join payment_points_df p on p.member_id = l.member_id")

# COMMAND ----------

loan_default_pts.createOrReplaceTempView("loan_default_points_df")

# COMMAND ----------

loan_df.createOrReplaceTempView("loan_details")

# COMMAND ----------

account_df.createOrReplaceTempView("account_details")

# COMMAND ----------

financial_df = spark.sql("select ldef.*, \
    case \
        when lower(l.loan_status) like '%fully paid%' then ${spark.sql.excellent_rated_pts} \
        when lower(l.loan_status) like '%current%' then ${spark.sql.good_rated_pts} \
        when lower(l.loan_status) like '%in grace period%' then ${spark.sql.bad_rated_pts} \
        when lower(l.loan_status) like '%late (16-30 days)%' or lower(l.loan_status) like '%late (31-120)%' then ${spark.sql.very_bad_rated_pts} \
        when lower(l.loan_status) like '%charged off%' then ${spark.sql.unacceptable_rated_pts} \
    end as loan_status_pts, \
    case \
        when lower(a.home_ownership) like '%own%' then ${spark.sql.excellent_rated_pts} \
        when lower(a.home_ownership) like '%rent%' then ${spark.sql.good_rated_pts} \
        when lower(a.home_ownership) like '%mortgage%' then ${spark.sql.bad_rated_pts} \
        when lower(a.home_ownership) like '%any%' or lower(a.home_ownership) is null then ${spark.sql.very_bad_rated_pts} \
        end as home_pts, \
    case \
        when l.funded_amount <= (a.total_high_credit_limit * 0.10) then ${spark.sql.excellent_rated_pts} \
        when l.funded_amount > (a.total_high_credit_limit * 0.10) and l.funded_amount <=(a.total_high_credit_limit * 0.20) then ${spark.sql.very_good_rated_pts} \
        when l.funded_amount > (a.total_high_credit_limit * 0.20) and l.funded_amount <=(a.total_high_credit_limit * 0.30) then ${spark.sql.good_rated_pts} \
        when l.funded_amount > (a.total_high_credit_limit * 0.30) and l.funded_amount <=(a.total_high_credit_limit * 0.50) then ${spark.sql.bad_rated_pts} \
        when l.funded_amount > (a.total_high_credit_limit * 0.50) and l.funded_amount <=(a.total_high_credit_limit * 0.70) then ${spark.sql.very_bad_rated_pts} \
        when l.funded_amount > (a.total_high_credit_limit * 0.70) then ${spark.sql.unacceptable_rated_pts} \
        end as credit_limit_pts, \
    case \
        when (a.grade) = 'A' and (a.sub_grade)='A1' then ${spark.sql.excellent_rated_pts} \
        when (a.grade) = 'A' and (a.sub_grade)='A2' then (${spark.sql.excellent_rated_pts}*0.80) \
        when (a.grade) = 'A' and (a.sub_grade)='A3' then (${spark.sql.excellent_rated_pts}*0.60) \
        when (a.grade) = 'A' and (a.sub_grade)='A4' then (${spark.sql.excellent_rated_pts}*0.40) \
        when (a.grade) = 'A' and (a.sub_grade)='A5' then (${spark.sql.excellent_rated_pts}*0.20) \
        when (a.grade) = 'B' and (a.sub_grade)='B1' then ${spark.sql.very_good_rated_pts} \
        when (a.grade) = 'B' and (a.sub_grade)='B2' then (${spark.sql.very_good_rated_pts}*0.80) \
        when (a.grade) = 'B' and (a.sub_grade)='B3' then (${spark.sql.very_good_rated_pts}*0.60) \
        when (a.grade) = 'B' and (a.sub_grade)='B4' then (${spark.sql.very_good_rated_pts}*0.40) \
        when (a.grade) = 'B' and (a.sub_grade)='B5' then (${spark.sql.very_good_rated_pts}*0.20) \
        when (a.grade) = 'C' and (a.sub_grade)='C1' then ${spark.sql.good_rated_pts} \
        when (a.grade) = 'C' and (a.sub_grade)='C2' then (${spark.sql.good_rated_pts}*0.80) \
        when (a.grade) = 'C' and (a.sub_grade)='C3' then (${spark.sql.good_rated_pts}*0.60) \
        when (a.grade) = 'C' and (a.sub_grade)='C4' then (${spark.sql.good_rated_pts}*0.40) \
        when (a.grade) = 'C' and (a.sub_grade)='C5' then (${spark.sql.good_rated_pts}*0.20) \
        when (a.grade) = 'D' and (a.sub_grade)='D1' then ${spark.sql.bad_rated_pts} \
        when (a.grade) = 'D' and (a.sub_grade)='D2' then (${spark.sql.bad_rated_pts}*0.80) \
        when (a.grade) = 'D' and (a.sub_grade)='D3' then (${spark.sql.bad_rated_pts}*0.60) \
        when (a.grade) = 'D' and (a.sub_grade)='D4' then (${spark.sql.bad_rated_pts}*0.40) \
        when (a.grade) = 'D' and (a.sub_grade)='D5' then (${spark.sql.bad_rated_pts}*0.20) \
        when (a.grade) = 'E' and (a.sub_grade)='E1' then ${spark.sql.very_bad_rated_pts} \
        when (a.grade) = 'E' and (a.sub_grade)='E2' then (${spark.sql.very_bad_rated_pts}*0.80) \
        when (a.grade) = 'E' and (a.sub_grade)='E3' then (${spark.sql.very_bad_rated_pts}*0.60) \
        when (a.grade) = 'E' and (a.sub_grade)='E4' then (${spark.sql.very_bad_rated_pts}*0.40) \
        when (a.grade) = 'E' and (a.sub_grade)='E5' then (${spark.sql.very_bad_rated_pts}*0.60) \
        when (a.grade) in ('F','G') and (a.sub_grade) in ('F1','G1') then ${spark.sql.unacceptable_rated_pts} \
        when (a.grade) in ('F','G') and (a.sub_grade) in ('F2','G2') then (${spark.sql.unacceptable_rated_pts}*0.80) \
        when (a.grade) in ('F','G') and (a.sub_grade) in ('F3','G3') then (${spark.sql.unacceptable_rated_pts}*0.60) \
        when (a.grade) in ('F','G') and (a.sub_grade) in ('F4','G4') then (${spark.sql.unacceptable_rated_pts}*0.40) \
        when (a.grade) in ('F','G') and (a.sub_grade) in ('F5','G5') then (${spark.sql.unacceptable_rated_pts}*0.20) \
        end as grade_pts \
    from loan_default_points_df ldef \
    left join loan_details l on ldef.member_id = l.member_id \
    left join account_details a on a.member_id = ldef.member_id")




# COMMAND ----------

financial_df.createOrReplaceTempView("loan_score_details")

# COMMAND ----------

loan_score = spark.sql("select member_id,first_name,last_name,state,country, \
    ((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, \
    ((delinq_pts+public_records_pts+public_bankruptcies_pts+enq_pts+hardship_pts)*0.45) as defaulters_history_pts, \
    ((loan_status_pts+home_pts+credit_limit_pts+grade_pts)*0.35) as financial_health_pts \
    from loan_score_details")

# COMMAND ----------

loan_score.createOrReplaceTempView("loan_score_pts")

# COMMAND ----------

loan_score_final = spark.sql("select ls.member_id,ls.first_name,ls.last_name,ls.state,ls.country, \
    (payment_history_pts+defaulters_history_pts+financial_health_pts) as loan_score \
    from loan_score_pts ls")

# COMMAND ----------

loan_score_final.createOrReplaceTempView("loan_score_eval")

# COMMAND ----------

loan_score_final=spark.sql("select ls.*, \
case \
WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN '" + excellent_grade + "' \
WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN '" + very_good_grade + "' \
WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN '" + good_grades + "' \
WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score > ${spark.sql.very_bad_grade_pts} THEN '" + bad_grade + "' \
WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN '" + very_bad_grade + "' \
WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN '" + unacceptable_grade + "' \
end as loan_final_grade \
from loan_score_eval ls")

# COMMAND ----------

loan_score_final.createOrReplaceTempView("loan_final_table")

# COMMAND ----------

spark.sql("select * from loan_final_table where loan_final_grade in ('B') ").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lending_loan_e2e.customers_loan_score
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/kushagrafinance/processed-data/lending_loan/customer_transformations/customers_loan_score'
# MAGIC select * from loan_final_table

# COMMAND ----------

