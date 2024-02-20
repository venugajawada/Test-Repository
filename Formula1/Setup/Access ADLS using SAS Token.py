# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS data using SAS Token 
# MAGIC 1.connect to ADLS through SAS Token
# MAGIC 1.list the files in demo container
# MAGIC 1.Read the file
# MAGIC
# MAGIC

# COMMAND ----------

f1_sas_token = dbutils.secrets.get(scope = 'f1-sas-token',key = 'f1-SAS')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.venustorage1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.venustorage1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.venustorage1.dfs.core.windows.net",f1_sas_token)


# COMMAND ----------

dbutils.fs.ls("abfss://demo@venustorage1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@venustorage1.dfs.core.windows.net"))
