# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_teste.silver_teste;
# MAGIC CREATE VOLUME IF NOT EXISTS projeto_teste.silver_teste.checkpoints;

# COMMAND ----------

from pyspark.sql.functions import col, to_date


checkpoint_path = "/Volumes/projeto_teste/silver_teste/checkpoints/stream_past_rates/"

df_past_rates_bronze = (spark.readStream
    .format("delta")
    .table("projeto_teste.bronze_teste.past_rates")
)

#Tipagem correta das colunas
df_silver = (df_past_rates_bronze
    .withColumn("ID_listing", col("listing_id").cast("bigint"))
    .withColumn("listing_date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("vacant_days", col("vacant_days").cast("bigint"))
    .withColumn("reserved_days", col("reserved_days").cast("bigint"))
    .withColumn("occupancy_rate", col("occupancy").cast("double"))
    .withColumn("revenue", col("revenue").cast("double"))
    .withColumn("avg_daily_rate", col("rate_avg").cast("double"))
    .withColumn("avg_booked_rate", col("booked_rate_avg").cast("double"))
    .withColumn("booking_leadtime_days_avg", col("booking_lead_time_avg").cast("bigint"))
    .withColumn("length_of_stay_avg", col("length_of_stay_avg").cast("bigint"))
    .withColumn("min_nights_avg", col("min_nights_avg").cast("bigint"))
    .withColumn("native_booked_rate_avg", col("native_booked_rate_avg").cast("double"))
    .withColumn("native_rate_avg", col("native_rate_avg").cast("double"))
    .withColumn("native_revenue", col("native_revenue").cast("double"))
    .withColumn("country", col("country").cast("string"))
    .withColumn("state", col("state").cast("string"))
    .withColumn("city", col("city").cast("string"))

    #Filtro de nulo de colunas relevantes
    .filter(col("listing_id").isNotNull())
    .filter(col("date").isNotNull())

    #Remoção de colunas desnecessárias na silver
    .drop("_rescued_data")
    .drop("ingestion_date")
    .drop("file_path")

    #Remoção de colunas renomeadas
    .drop("listing_id", "date", "occupancy", "rate_avg", "booked_rate_avg", "booking_lead_time_avg")
)

query_silver = (df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}data")
    .trigger(availableNow=True)
    .toTable("projeto_teste.silver_teste.past_rates")
)

print("Tabela criada na camada silver com suscesso")
