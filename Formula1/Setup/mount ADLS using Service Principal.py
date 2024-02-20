# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS data using SAS Token 
# MAGIC 1.connect to ADLS through SAS Token
# MAGIC 1.list the files in demo container
# MAGIC 1.Read the file
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-clientid')
tenant_id = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-tenantid')
client_secret = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@venustorage1.dfs.core.windows.net/",
  mount_point = "/mnt/venustorage1/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/venustorage1/demo")

# COMMAND ----------

display(spark.read.csv("/mnt/venustorage1/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())
