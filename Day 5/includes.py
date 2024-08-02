# Databricks notebook source
input = "dbfs:/mnt/hexawaredatabricks/raw/input_files/1000_richest_people_in_the_world.csv"
output="dbfs:/mnt/hexawaredatabricks/raw/output_files/"

# COMMAND ----------

def add_ingestion(a):
    b=a.withColumn("ingestion_date",current_timestamp())
    return b

# COMMAND ----------

# from pyspark.sql.types import *
# pyspark_schmema= StructType([StructField("name",StringType()),
#                              StructField("country",StringType()),
#                              StructField("industry",StringType()),
#                              StructField("net_worth",DoubleType()),
#                              StructField("company",StringType())
# ])

# COMMAND ----------

#df_new2=spark.read.schema(pyspark_schmema).csv(f"{input}",header=True)

# COMMAND ----------


