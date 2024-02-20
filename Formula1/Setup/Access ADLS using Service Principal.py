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


spark.conf.set("fs.azure.account.auth.type.venustorage1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.venustorage1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.venustorage1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.venustorage1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.venustorage1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://demo@venustorage1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@venustorage1.dfs.core.windows.net"))
