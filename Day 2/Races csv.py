# Databricks notebook source
# MAGIC %run "/Workspace/Users/bhupalk315@outlook.com/Hexaware_training/params"

# COMMAND ----------

# DBTITLE 1,Reading
race_df = spark.read.csv(f"{input_path}races.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Tranforming
from pyspark.sql.functions import *
new_df=race_df.withColumn("ingestion_date",current_date())
new_race_df=new_df.withColumnRenamed("raceid","race_id").withColumnRenamed("circuitid","circuit_id")
final_df = new_race_df.withColumn("path",input_file_name())
df= final_df.drop('url')

# COMMAND ----------

# DBTITLE 1,writing
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.races")

# COMMAND ----------

dbutils.notebook.exit('0')
