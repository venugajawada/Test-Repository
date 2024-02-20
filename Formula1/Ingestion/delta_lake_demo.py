# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_delta
# MAGIC LOCATION '/mnt/venustorage1/deltademo'

# COMMAND ----------

results_df = spark.read \
    .option('inferSchema', True) \
    .json("/mnt/venustorage1/raw/results.json")    

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.results_managed")
                                                            


# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_delta.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/venustorage1/deltademo/results_external")
