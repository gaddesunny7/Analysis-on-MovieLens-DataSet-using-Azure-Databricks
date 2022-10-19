# Databricks notebook source
spark

# COMMAND ----------

#Connect To Azure DataLakeStorage
spark.conf.set(
            "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
            "<Generate Token>"
            )

# COMMAND ----------

#Access the files in the container through the Data Lake Storage Account
dbutils.fs.ls("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/")

# COMMAND ----------

#Commenting it out because it will be used in other notebook
#dbutils.fs.mount(
#source = "wasbs://<storage-container-name>@<storage-account-name>.blob.core.windows.net",
#mount_point = "/mnt/<storage-container-name>",
#extra_configs = {"fs.azure.sas.<storage-container-name>.<storage-account-name>.blob.core.windows.net":"<Generate Token>"})

# COMMAND ----------

#List all the mount files present 
dbutils.fs.ls('/mnt/')

# COMMAND ----------

#Unmount a file in the DBFS
#dbutils.fs.unmount("/mnt/<storage-container-name>/")
