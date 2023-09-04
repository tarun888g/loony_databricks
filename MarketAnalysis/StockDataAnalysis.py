# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

df = spark.read.format("delta") \
            .load("dbfs:/user/hive/warehouse/tesla_stock_price")
display(df)

# COMMAND ----------

df.select(max("Adj_Close"),max("Volume")) \
    .withColumnRenamed("max(Adj_Close)", "Max_Close") \
    .withColumnRenamed("max(Volume)","Max_Volume") \
    .show(truncate=False)

# COMMAND ----------
df.select("Date","Adj_Close","Volume") \
    .where("Volume > 150000000") \
    .write.option("header","true") \
    .mode('overwrite') \
    .csv("dbfs:/FileStore/outputs/tesla_highvoldays.csv")


# COMMAND ----------



# COMMAND ----------


