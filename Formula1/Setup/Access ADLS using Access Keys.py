# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS data using Cluster Scoped Credentials
# MAGIC 1.connect to ADLS through Access Key
# MAGIC 1.list the files in demo container
# MAGIC 1.Read the file
# MAGIC
# MAGIC

# COMMAND ----------

f1_account_key = dbutils.secrets.get(scope = 'f1-scope',key = 'f1-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.venustorage1.dfs.core.windows.net",
f1_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@venustorage1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@venustorage1.dfs.core.windows.net"))
