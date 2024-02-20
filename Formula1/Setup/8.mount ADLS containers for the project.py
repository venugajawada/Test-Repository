# Databricks notebook source
def mount_adls(storage_account_name,container_name):
    #retrieve secrets
    client_id = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-clientid')
    tenant_id = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-tenantid')
    client_secret = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-client-secret')
    # set config
    configs = { "fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # mount containers
    
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    display(dbutils.fs.mounts())


    

# COMMAND ----------

mount_adls('venustorage1','deltademo')

# COMMAND ----------

dbutils.fs.ls('/mnt/venustorage1/deltademo')
