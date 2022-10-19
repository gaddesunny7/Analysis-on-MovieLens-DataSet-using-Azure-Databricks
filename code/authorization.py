# Databricks notebook source
spark

# COMMAND ----------

#Connect To Azure DataLakeStorage
spark.conf.set(
            "fs.azure.account.key.sadevdatalakeaz.dfs.core.windows.net",
            "cYaqBCIC7Njfa54+EpF5o0MRIm4ePv9pQT6fAKbSdgnNl25z7LQS7M4xnXI0t6zpCMl2EuNxhl16+AStgcF85g=="
            )

# COMMAND ----------

#Access the files in the container through the Data Lake Storage Account
dbutils.fs.ls("abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/")

# COMMAND ----------

#Commenting it out because it will be used in other notebook
#dbutils.fs.mount(
#source = "wasbs://sacondatalakedevaz@sadevdatalakeaz.blob.core.windows.net",
#mount_point = "/mnt/sacondatalakedevaz",
#extra_configs = {"fs.azure.sas.sacondatalakedevaz.sadevdatalakeaz.blob.core.windows.net":"sp=r&st=2022-10-16T15:30:05Z&se=2022-10-16T23:30:05Z&spr=https&sv=2021-06-08&sr=c&sig=4JSrCd2KqxSuudFMqgMl41RXRDHsUgZNN3GZeUv1HEQ%3D"})

# COMMAND ----------

#List all the mount files present 
dbutils.fs.ls('/mnt/')

# COMMAND ----------

#Unmount a file in the DBFS
#dbutils.fs.unmount("/mnt/azdwsmovielens/")