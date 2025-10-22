# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

import os
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)



# COMMAND ----------

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC # DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTOLOADER

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimUser/checkpoint")\
            .option("schemaEvolutionMode", "addNewColumns")\
           .load("abfss://bronze@storageaccdivy.dfs.core.windows.net/DimUser")


# COMMAND ----------

df_user = df.withColumn("user_name", upper(col("user_name")) )
display(df_user)


# COMMAND ----------

df_user_obj = reusable()


df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimUser/checkpoint")\
.trigger(once = True)\
.option("path","abfss://silver@storageaccdivy.dfs.core.windows.net/DimUser/data")\
.toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimArtist/checkpoint")\
            .option("schemaEvolutionMode", "addNewColumns")\
           .load("abfss://bronze@storageaccdivy.dfs.core.windows.net/DimArtist")

display(df_art)

# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])
df_art.dropDuplicates(['artist_id'])
display(df_art)



# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimArtist/checkpoint")\
.trigger(once = True)\
.option("path","abfss://silver@storageaccdivy.dfs.core.windows.net/DimArtist/data")\
.toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimTrack/checkpoint")\
            .option("schemaEvolutionMode", "addNewColumns")\
           .load("abfss://bronze@storageaccdivy.dfs.core.windows.net/DimTrack")



# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col("duration_sec")< 150, "low")\
                                               .when(col("duration_sec")< 300, "medium")\
                                                .otherwise("high"))
df_track = df_track.withColumn("track_Name", regexp_replace(col("track_name"), "-", " "))

df_track = reusable().dropColumns(df_track, columns=['_rescued_data'])

display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimTrack/checkpoint")\
.trigger(once = True)\
.option("path","abfss://silver@storageaccdivy.dfs.core.windows.net/DimTrack/data")\
.toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimDate/checkpoint")\
            .option("schemaEvolutionMode", "addNewColumns")\
           .load("abfss://bronze@storageaccdivy.dfs.core.windows.net/DimDate")



# COMMAND ----------

display(df_date)

# COMMAND ----------

df_date = reusable().dropColumns(df_date, columns=['_rescued_data'])


# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/DimDate/checkpoint")\
.trigger(once = True)\
.option("path","abfss://silver@storageaccdivy.dfs.core.windows.net/DimDate/data")\
.toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC # FactStream

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/FactStream/checkpoint")\
            .option("schemaEvolutionMode", "addNewColumns")\
           .load("abfss://bronze@storageaccdivy.dfs.core.windows.net/FactStream")


# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, columns=['_rescued_data'])

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageaccdivy.dfs.core.windows.net/FactStream/checkpoint")\
.trigger(once = True)\
.option("path","abfss://silver@storageaccdivy.dfs.core.windows.net/FactStream/data")\
.toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

