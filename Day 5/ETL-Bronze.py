# Databricks notebook source
# MAGIC %run "/Workspace/Users/bhupalk315_outlook.com#ext#@bhupalk315outlook.onmicrosoft.com/Bhupal/Day 5/includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import *
df1=add_ingestion(df)
df1.columns

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company','ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.write.mode("overwrite").save(f"{output}bhupal/richest")

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))

# COMMAND ----------

# DBTITLE 1,Writing to ADLS
df3.write.mode("overwrite").option("mergeSchema","true").save(f"{output}bhupal/richest")

# COMMAND ----------

# DBTITLE 1,Writing To Managed Table
df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("katruhexaware.bronze.richest_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from katruhexaware.bronze.richest_bronze

# COMMAND ----------


