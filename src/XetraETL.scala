// Databricks notebook source
// MAGIC %md
// MAGIC ## Xetra Trade Data ETL
// MAGIC This notebook contains the source code for pipelining the Xetra trade dataset into "biggest winner", "most traded", and "highest volume" report tables.
// MAGIC 
// MAGIC If this was a production ETL pipeline, we would use merge or upsert (update and insert) to update data as it comes in each day instead of overwriting the whole table.
// MAGIC 
// MAGIC We would also write tests for incoming data as well as after the transformations to make sure that only quality data gets stored.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// COMMAND ----------

val inputPath = "/FileStore/data/xetra"
val outputPath = "/delta/xetra"

// COMMAND ----------

// Convert data into delta format partitioned by date
spark.read.format("csv")
.option("header", "true")
.load(inputPath)
.write
.format("delta")
.mode("overwrite")
.partitionBy("date")
.save(outputPath)

// COMMAND ----------

// Read data from DBFS
spark.sql(s"""
  DROP TABLE IF EXISTS xetra.raw 
""")

spark.sql(s"""
  CREATE TABLE xetra.raw 
  USING DELTA
  LOCATION "$outputPath"
""")

spark.sql(s"""
  OPTIMIZE xetra.raw
""")

// COMMAND ----------

val rawDF = spark.sql("select * from xetra.raw")

// Filter for only transactions using EUR currency and rename columns
val df = rawDF
.filter($"Currency" === "EUR")
.select(
  col("ISIN").as("isin"), 
  col("Mnemonic").as("mnemonic"), 
  col("SecurityDesc").as("security_desc"),
  col("SecurityType").as("security_type"), 
  col("Currency").as("currency"), 
  col("SecurityID").as("security_id").cast("int"),
  col("Date").as("date"), 
  col("Time").as("time").cast("string"),
  col("StartPrice").as("start_price").cast("float"),
  col("MaxPrice").as("max_price").cast("float"),
  col("MinPrice").as("min_price").cast("float"),
  col("EndPrice").as("end_price").cast("float"),
  col("TradedVolume").as("traded_volume").cast("float"),
  col("NumberOfTrades").as("number_of_trades").cast("int")
)
df.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Security

// COMMAND ----------

// Store detailed security information in security query table
val security = df.select(
  "security_id", "isin", "mnemonic", "security_type", "security_desc"
)
.distinct()
.orderBy("security_id")

security.write.format("delta").mode("overwrite").saveAsTable("xetra.security");

display(security)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Transaction

// COMMAND ----------

// Store transaction data in transaction query table
val transaction = df
.select(
  "date", "time", "security_id", "currency", "start_price", "max_price", 
  "min_price", "end_price", "traded_volume", "number_of_trades"
)
.orderBy("date", "time")

// Cache to optimize transformations
transaction.cache()

transaction.write.format("delta").mode("overwrite").saveAsTable("xetra.transaction");

display(transaction)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Highest Volume

// COMMAND ----------

def highestVolumeByDay(df: DataFrame): DataFrame = {
  // Implied volume formula: avg((high - low) / low)
  val implied_volume: (Column, Column) => (Column) = (high, low) => { (high - low) / low }

  // Implied volumes for all transactions
  val allvolumes = df
  .groupBy("date", "security_id")
  .agg(avg(implied_volume(col("max_price"), col("min_price"))).as("implied_volume"))
  .orderBy(desc("implied_volume"))
  .select("date", "security_id", "implied_volume")
  
  // Max volume for each date
  val maxvolumes = allvolumes
  .groupBy("date")
  .agg(max("implied_volume").as("implied_volume"))
  .select("date", "implied_volume")

  // Highest volume by day
  allvolumes
  .join(maxvolumes, allvolumes("date") === maxvolumes("date") && allvolumes("implied_volume") === maxvolumes("implied_volume"))
  .select(
    allvolumes("date"),
    allvolumes("security_id"),
    allvolumes("implied_volume").cast("float")
  )
  .orderBy("date")
}

val highestVolume = highestVolumeByDay(transaction)
highestVolume.cache()
display(highestVolume)

// COMMAND ----------

highestVolume.write.format("delta").mode("overwrite").saveAsTable("xetra.highest_volume");

// COMMAND ----------

// MAGIC %md
// MAGIC ### Most Traded

// COMMAND ----------

def mostTradedByDay(df: DataFrame): DataFrame = {
  
  // Trade amount for each transaction
  val sumtable = df
  .groupBy("security_id", "date")
  .agg(sum(col("start_price") * col("traded_volume")).as("trade_amount_mm"))
  .orderBy(desc("trade_amount_mm"))
  
  // Max trade amount for each day
  val maxtable = sumtable
  .groupBy("date")
  .agg(max("trade_amount_mm").as("max_amount"))
  .select("date", "max_amount")
  
  // Most traded by day
  sumtable
  .join(maxtable, sumtable("trade_amount_mm") === maxtable("max_amount") && sumtable("date") === maxtable("date"))
  .select(
    sumtable("date"), 
    sumtable("security_id"), 
    (maxtable("max_amount") / 1e6).as("trade_amount_mm")
  )
  .orderBy("date")
}
  
val mostTraded = mostTradedByDay(transaction)
mostTraded.cache()
display(mostTraded)

// COMMAND ----------

mostTraded.write.format("delta").mode("overwrite").saveAsTable("xetra.most_traded");

// COMMAND ----------

// MAGIC %md
// MAGIC ### Biggest Winner

// COMMAND ----------

def biggestWinnerByDay(df: DataFrame): DataFrame = {
  val percentchange: (Column, Column) => (Column) = (endprice, startprice) => { (endprice - startprice) / startprice }

  // Timestamps for the first and last transaction grouped by date and security
  val timestamps = df
  .groupBy("security_id", "date")
  .agg(min("time").as("min_time"), max("time").as("max_time"))
  .select("date", "security_id", "min_time", "max_time")
  timestamps.cache()

  // Price at the first transaction
  val minval = df
  .join(timestamps, timestamps("security_id") === df("security_id") && df("date") === timestamps("date"))
  .where(df("time") === timestamps("min_time"))
  .select(df("start_price"), timestamps("security_id"), timestamps("min_time"), df("date"))

  // Price at the last transaction
  val maxval = df
  .join(timestamps, timestamps("security_id") === df("security_id") && df("date") === timestamps("date"))
  .where(df("time") === timestamps("max_time"))
  .select(df("end_price"), timestamps("security_id"), timestamps("max_time"), df("date"))

  // Percent change for all transactions
  val allreturns = minval
  .join(maxval, minval("security_id") === maxval("security_id") && minval("date") === maxval("date"))
  .select(minval("date"), minval("security_id"), percentchange(col("end_price"), minval("start_price")).as("percent_change"))

  // Max percent change for each day
  val maxreturns = allreturns
  .groupBy("date")
  .agg(max("percent_change").as("percent_change"))
  .select("date", "percent_change")

  // Biggest winner by day
  allreturns
  .join(maxreturns, maxreturns("percent_change") === allreturns("percent_change") && maxreturns("date") === allreturns("date"))
  .select(allreturns("date"), allreturns("security_id"), allreturns("percent_change").cast("float"))
  .orderBy("date")
}

val biggestWinner = biggestWinnerByDay(transaction)
biggestWinner.cache()
display(biggestWinner)

// COMMAND ----------

biggestWinner.write.format("delta").mode("overwrite").saveAsTable("xetra.biggest_winner");
