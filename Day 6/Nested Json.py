# Databricks notebook source
df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.createOrReplaceTempView("batter_vw")

# COMMAND ----------

df1.where("batters_type='Chocolate' and topping_id=5001" ).display()

# COMMAND ----------


