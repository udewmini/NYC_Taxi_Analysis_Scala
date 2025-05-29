// Databricks notebook source
// load data
val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/FileStore/tables/sample.csv")

// COMMAND ----------

// Preview
df.show(5)

// COMMAND ----------

//Drop rows with nulls
val cleanDF = df.na.drop()

// Group by pickuplocatione
val pickupStats = cleanDF
  .groupBy("PULocationID")
  .count()
  .orderBy($"count".desc)

// Show top 10 pickup locations
pickupStats.show(10)

// COMMAND ----------

val top10 = pickupStats.limit(10)
top10.createOrReplaceTempView("top_pickups")


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM top_pickups
// MAGIC
// MAGIC