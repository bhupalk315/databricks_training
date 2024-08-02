# Databricks notebook source
# MAGIC %sql
# MAGIC Create or replace table katruhexaware.silver.richest_silver as
# MAGIC select name, country, industry, net_worth_in_billions, company from katruhexaware.bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from katruhexaware.silver.richest_silver

# COMMAND ----------


