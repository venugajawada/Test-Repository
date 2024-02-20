# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope='f1-scope',key='f1-account-key')

# COMMAND ----------

dbutils.secrets.list(scope='f1-scope')
