# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,DecimalType,StructField,StructType,datetime


# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                          StructField("round",IntegerType(),True),
                          StructField("circuitid",IntegerType(),True),
                          StructField("name",StringType(),True),
                          StructField("date",StringType(),True),
                          StructField("time",StringType(),True)
                          
                         ] )

# COMMAND ----------

races_df = spark.read.option("header", True).csv("dbfs:/mnt/venustorage1/raw/races.csv")
display(races_df)

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("raceid","race_id")\
                            .withColumnRenamed("circuitid","circuit_id")\
                            .withColumnRenamed("year","race_year")
display(races_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_renamed_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("date"),col("time"))

display(races_selected_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import current_timestamp,concat,lit,col
 


# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

races_transformed_df = races_selected_df.withColumn("ingestion_date", current_timestamp())\
                   .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                   .withColumn("data_source",lit(v_data_source))

display(races_transformed_df)

# COMMAND ----------

races_transformed_df.write.mode("overwrite").partitionBy("race_year").parquet("dbfs:/mnt/venustorage1/processed/races")



# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/venustorage1/processed/races"))
