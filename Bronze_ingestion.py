# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_teste.bronze_teste;
# MAGIC CREATE VOLUME IF NOT EXISTS projeto_teste.bronze_teste.checkpoints;
# MAGIC CREATE VOLUME IF NOT EXISTS projeto_teste.bronze_teste.landingzone;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

volume_path = "/Volumes/projeto_teste/bronze_teste/landingzone/"
checkpoint_path = "/Volumes/projeto_teste/bronze_teste/checkpoints/"
table_path = "projeto_teste.bronze_teste"


df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.schemaLocation",f"{checkpoint_path}stream_past_rates/schema")
    .option("cloudFiles.schemaEvolutionMode","rescue")
    .load(f"{volume_path}json_past_rates/")
)

df_stream = (df_stream
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("file_path", input_file_name())
)

query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",f"{checkpoint_path}stream_past_rates/data")
    .trigger(availableNow=True)
    .toTable(f"{table_path}.past_rates")
)

print("Ingestão da tabela past_rates na camada bronze concluída")


# COMMAND ----------

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.schemaLocation",f"{checkpoint_path}stream_listings/schema")
    .option("cloudFiles.schemaEvolutionMode","rescue")
    .load(f"{volume_path}json_listings/")
)

df_stream = (df_stream
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("file_path", input_file_name())
)

query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",f"{checkpoint_path}stream_listings/data")
    .trigger(availableNow=True)
    .toTable(f"{table_path}.listings")
)

print("Ingestão da tabela listings na camada bronze concluída")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from projeto_teste.bronze_teste.listings;
# MAGIC
