# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

from pyspark.sql.types import *

users_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("Name", StringType()),
    StructField("Gender", StringType()),
    StructField("Salary", IntegerType()),
    StructField("Country", StringType()),
    StructField("Date", StringType())
])

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/bhupal/stream")
.trigger(once=True)
.table("katruhexaware.bronze_stream.stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from katruhexaware.bronze_stream.stream
