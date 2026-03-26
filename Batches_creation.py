# Databricks notebook source
df_listings = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/listings.csv", header=True)
df_past_rates = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/past_rates.csv", header=True)

#Salvar apenas uma parte do dataset para funcionamento do autoloader na bronze
batch_listings_1 = df_listings.limit(1000)
batch_path_rates_1 = df_past_rates.limit(1000)


(batch_listings_1.write
    .format("json")
    .mode("append")
    .save("/Volumes/projeto_teste/bronze_teste/landingzone/json_listings/"))

(batch_path_rates_1.write
    .format("json")
    .mode("append")
    .save("/Volumes/projeto_teste/bronze_teste/landingzone/json_past_rates/"))


print("Dados da leva 1 salvos")

# COMMAND ----------

from pyspark.sql.functions import lit

df_listings = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/listings.csv", header=True)
df_past_rates = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/past_rates.csv", header=True)


listings_salvos = spark.read.json("/Volumes/projeto_teste/bronze_teste/landingzone/json_listings")
past_rates_salvos = spark.read.json("/Volumes/projeto_teste/bronze_teste/landingzone/json_past_rates")

batch_listings_2 = df_listings.exceptAll(listings_salvos).limit(1000)
path_rates_2 = df_past_rates.exceptAll(past_rates_salvos).limit(1000)


#Adiciona coluna extra ao schema para testar o "rescue" do autoloader
batch_path_rates_2 = path_rates_2.withColumn("extra_tax", lit("unespected column"))


(batch_listings_2.write
.format("json")
.mode("append")
.save("/Volumes/projeto_teste/bronze_teste/landingzone/json_listings/"))

(batch_path_rates_2.write
.format("json")
.mode("append")
.save("/Volumes/projeto_teste/bronze_teste/landingzone/json_past_rates/"))

print("Dados da leva 2 salvos")
