# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

bucket_name = "gs://jfkmount"
mount_name = "my-mount"
dbutils.fs.unmount(
  f"/mnt/databricks/{mount_name}"
)

# COMMAND ----------

spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", "{{secrets/SecretBucket/client_email}}")
spark.conf.set("spark.hadoop.fs.gs.project.id", "{{secrets/SecretBucket/project_id}}")
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", "{{secrets/SecretBucket/private_key}}")
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", "{{secrets/SecretBucket/private_key_id}}")

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks

# COMMAND ----------

bucket_name = "gs://jfkmount"
mount_name = "my-mount"
dbutils.fs.mount(
  f"gs://{bucket_name}",
  f"/mnt/databricks/{mount_name}",
  extra_configs = {"fs.gs.project.id": "quixotic-geode-437008-m8"}
)

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dl/demo')

# COMMAND ----------


