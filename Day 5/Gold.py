# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists katruhexaware.gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table katruhexaware.gold.country_count as
# MAGIC select country, count(country) as count from katruhexaware.silver.richest_silver group by country order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from katruhexaware.gold.country_count
