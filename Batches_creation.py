# Databricks notebook source
df_listings = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/listings.csv", header=true, infesSchema=True)
df_past_rates = spark.read.csv("/Volumes/projeto_teste/bronze_teste/landingzone/past_rates.csv", header=true, infesSchema=True)

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

batch_listings_2 = df_listings.exceptAll(batch_listings_1).limit(1000)
batch_path_rates_2 = df_past_rates.exceptAll(batch_path_rates_1).limit(1000)


(batch_listings_2.write
.format("json")
.mode("append")
.save("/Volumes/projeto_teste/bronze_teste/landingzone/json_listings/"))

(batch_path_rates_2.write
.format("json")
.mode("append")
.save("/Volumes/projeto_teste/bronze_teste/landingzone/json_past_rates/"))

print("Dados da leva 2 salvos")
