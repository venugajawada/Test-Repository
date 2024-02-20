# Databricks notebook source
# MAGIC %md
# MAGIC Read CSV files using Spark Read Dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType,DoubleType,StringType,StructField,StructType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField ("circuitId", IntegerType(),False),
                                       StructField ("circuitRef",StringType(),True),
                                       StructField ("name",StringType(),True),
                                       StructField ("location",StringType(),True),
                                       StructField ("country",StringType(),True),
                                       StructField ("lat",DoubleType(),True),
                                       StructField ("lng",DoubleType(),True),
                                       StructField ("alt",IntegerType(),True),
                                       StructField ("url",StringType(), True),                                                                      


] )                              

# COMMAND ----------

df = spark.read.\
option("header" , True).\
schema(circuits_schema).\
csv("dbfs:/mnt/venustorage1/raw/circuits.csv")
df.display()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = df.select(col("circuitId"),col("circuitRef"),col("name"),col('location'),
                                col("country").alias("race_country"),
                                col("lat"),col("lng"),col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC Rename Columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = circuits_selected_df.withColumn("ingested_date",current_timestamp())
display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("mnt/venustorge1/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/venustorge1/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/venustorge1/processed/circuits"))

