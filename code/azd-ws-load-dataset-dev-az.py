# Databricks notebook source
#run the authorization notebook using th run command 

# COMMAND ----------

# MAGIC %run /azd-fld-ws-dev-az/authorization

# COMMAND ----------

#Test to check the files in the container through the Data Lake Storage Account
dbutils.fs.ls("abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/")

# COMMAND ----------

#List all the mounts
dbutils.fs.mounts()

# COMMAND ----------

#copy the file using the DBFS
dbutils.fs.cp("dbfs:/mnt/sacondatalakedevaz/ml_latest_small.zip","file:/tmp/ml_latest_small.zip")

# COMMAND ----------

#List the contents in the temp folder using the bash script using the magic command %sh
#Unzip the contents from the temp folder and store it in the temp folder itself %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /tmp/ml_latest_small.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip  /tmp/ml_latest_small.zip -d /tmp

# COMMAND ----------

dbutils.fs.ls("file:/tmp/ml-latest-small/")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/ml-latest-small/movies.csv","abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/movies.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/tags.csv","abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/tags.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/links.csv","abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/links.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/ratings.csv","abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/ratings.csv")

# COMMAND ----------

#Another command to check the files in the adlsgen2 using the fs magic command

# COMMAND ----------

# MAGIC %fs ls abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/

# COMMAND ----------

# MAGIC %fs ls abfss://sacondatalakedevaz@sadevdatalakeaz.dfs.core.windows.net/

# COMMAND ----------

dbutils.fs.ls('/mnt/')