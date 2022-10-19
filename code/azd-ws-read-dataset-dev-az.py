# Databricks notebook source
#run the authorization notebook using th run command 

# COMMAND ----------

import datetime
import pyspark.sql.functions as f
import pyspark.sql.types
import pandas as pd

from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql.functions import unix_timestamp,from_unixtime
from pyspark.sql import Window
from pyspark.sql.functions import rank,min

# COMMAND ----------

#File Location and Type for movies,link,ratings
movies = '/mnt/<storage-container-name>/movies.csv'
link = '/mnt/<storage-container-name>/links.csv'
tags = '/mnt/<storage-container-name>/tags.csv'
ratings = '/mnt/<storage-container-name>/ratings.csv'
file_type = 'csv'


#CSV options
infer_schema = 'true'
first_row_is_header = 'true'
delimiter = ','

#The applied files are for CSV types, other files will be ignored

df_movies = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .load(movies)


df_link = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .load(link)



df_tags = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .load(tags)



df_ratings = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .load(ratings)

# COMMAND ----------

#display(df_movies.show(5))
#display(df_link.show(5))
#display(df_ratings.show(5))
#display(df_tags.show(5))

# COMMAND ----------

#join with ratings_table 
df_movies_ratings = df_movies.join(df_ratings,'movieId','left')

display(df_movies_ratings)

# COMMAND ----------

#check duplicates are present
df_dup_checks = df_movies_ratings.groupby('movieId').count()
display(df_dup_checks)

# COMMAND ----------

df_movies.where(df_movies.movieId.isin([296,593])).display()

# COMMAND ----------

#Join with Users Dataset
df_movies_ratings = df_movies_ratings.join(df_tags,['movieId','userId'],'inner')
display(df_movies_ratings)

# COMMAND ----------

df_rating_tags = df_ratings.join(df_tags,['movieId'],'inner')
display(df_rating_tags)

# COMMAND ----------

#Converting the timestamp column in a proper time format in a new column 
df_ratings_add_tsdate = df_ratings.withColumn("tsdate",f.from_unixtime("timestamp"))

# COMMAND ----------

display(df_ratings_add_tsdate)

# COMMAND ----------

df_select_ratings = df_ratings_add_tsdate.select('userId','movieId','rating',f.to_date(unix_timestamp('tsdate','yyyy-MM-dd HH:mm:ss')
                                                                                      .cast('timestamp')).alias('rating_date'))
display(df_select_ratings)

# COMMAND ----------

df_rating_year = df_select_ratings.groupby('rating_date').count()
df_rating_year.display()

# COMMAND ----------

df_avg_ratings = df_select_ratings.groupby('movieId').mean('rating')
df_avg_ratings.display()

# COMMAND ----------

df = df_avg_ratings.join(df_movies,['movieId'],'inner')
df = df.withColumnRenamed('avg(rating)','avg_rating')
display(df)

# COMMAND ----------

df_total_rating = df_select_ratings.groupby('movieId').count()
df_total_rating.display()

# COMMAND ----------

df_total_rating = df_total_rating.filter(df_total_rating['count'] > 5)
df_ratings_filtered = df_select_ratings.join(df_total_rating,'movieId','inner')
display(df_ratings_filtered)

# COMMAND ----------

df_ratings_per_user = df_ratings_filtered.select('userId','movieId','rating').groupby('userId','movieId').max('rating')
df_ratings_per_user_movie = df_ratings_per_user.join(df_movies,'movieId','inner')
df_ratings_per_user_movie =  df_ratings_per_user_movie.withColumnRenamed('max(rating)','max_rating')
display(df_ratings_per_user_movie)

# COMMAND ----------

df_rating_max = df_ratings_per_user_movie.groupby('userId','movieId','title','genres').max('max_rating')
df_rating_max = df_rating_max.withColumnRenamed('max(max_rating)','max_rating')
display(df_rating_max)

# COMMAND ----------

df_rating_max = df_rating_max.filter(df_rating_max['max_rating'] >= 4)
display(df_rating_max)

# COMMAND ----------

#identify best movies per genre
df_movies_per_genre = df_rating_max.groupby('genres','title').count()
display(df_movies_per_genre)

# COMMAND ----------

#identify genres of user
df_movies_per_user = df_rating_max.select('userId','title','genres').groupby('userId','genres').count()
display(df_movies_per_user)

# COMMAND ----------

#Latest Trending Movies Over all
df_recent_movie = df_ratings_filtered.groupby('userId','movieId').agg(f.max(df_ratings_filtered['rating_date']))
display(df_recent_movie)

# COMMAND ----------

df_movies_per_genre = df.groupby('genres').avg('avg_rating')
display(df_movies_per_genre)

# COMMAND ----------

