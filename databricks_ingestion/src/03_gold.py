# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_teste.gold_teste;

# COMMAND ----------

from pyspark.sql.functions import col, year, month, avg, sum, count

df_silver = spark.read.table("projeto_teste.silver_teste.past_rates")

df_gold = (df_silver
    #extrair métricas de data da coluna de data
    .withColumn("year", year("listing_date")) 
    .withColumn("month", month("listing_date"))

    #agrupando por localidade e data
    .groupBy("year", "month", "city", "country", "state")

    #agregar as métricas (por mês)
    .agg(
        avg("revenue").alias("avg_monthly_revenue"),
        avg("occupancy_rate").alias("avg_occupancy_rate"),
        sum("reserved_days").alias("total_reserved_days"),
        count("ID_listing").alias("total_active_listings")
    )

    .orderBy("year", "month", "country", "state", "city")
)


(df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("projeto_teste.gold_teste.monthy_city_listings_metrics")
)

print ("Gold criada com sucesso")
