# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS data using Access keys
# MAGIC 1.connect to ADLS through Access Key
# MAGIC 1.list the files in demo container
# MAGIC 1.Read the file
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.ls("abfss://demo@venustorage1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@venustorage1.dfs.core.windows.net"))
