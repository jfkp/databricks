# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

dbutils.fs.unmount('/mnt/databricks/my-mount')

# COMMAND ----------

dbutils.fs.unmount('/mnt/jfkmount/raw')

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    #client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    #tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    #client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')
    
    # Set spark configurations

    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", "{{secrets/SecretBucket/client_email}}")
    spark.conf.set("spark.hadoop.fs.gs.project.id", "{{secrets/SecretBucket/project_id}}")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", "{{secrets/SecretBucket/private_key}}")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", "{{secrets/SecretBucket/private_key_id}}")
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"gs://{storage_account_name}/{container_name}/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}")
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Raw Container

# COMMAND ----------

mount_adls('jfkmount','raw')

# COMMAND ----------

mount_adls('jfkmount','demo')

# COMMAND ----------

mount_adls('jfkmount', 'processed')

# COMMAND ----------

mount_adls('jfkmount', 'presentation')

# COMMAND ----------


