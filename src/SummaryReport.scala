// Databricks notebook source
// MAGIC %md
// MAGIC ## Xetra Trade Report (2018-02-01) - (2018-02-28)

// COMMAND ----------

// Get report tables and join with detailed security information

val biggestWinner = spark.sql(s"""
  SELECT a.date, b.security_id, b.isin, b.mnemonic, b.security_type, b.security_desc, a.percent_change
  FROM xetra.biggest_winner AS a
  JOIN xetra.security AS b
  ON a.security_id = b.security_id
  ORDER BY a.date
""")

val highestVolume = spark.sql(s"""
  SELECT a.date, b.security_id, b.isin, b.mnemonic, b.security_type, b.security_desc, a.implied_volume
  FROM xetra.highest_volume AS a
  JOIN xetra.security AS b
  ON a.security_id = b.security_id
  ORDER BY a.date
""")

val mostTraded = spark.sql(s"""
  SELECT a.date, b.security_id, b.isin, b.mnemonic, b.security_type, b.security_desc, a.trade_amount_mm
  FROM xetra.most_traded AS a
  JOIN xetra.security AS b
  ON a.security_id = b.security_id
  ORDER BY a.date
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Biggest Winner By Day

// COMMAND ----------

display(biggestWinner)

// COMMAND ----------

display(biggestWinner)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Highest Volume By Day

// COMMAND ----------

display(highestVolume)

// COMMAND ----------

display(highestVolume)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Most Traded By Day

// COMMAND ----------

display(mostTraded)

// COMMAND ----------

display(mostTraded)

// COMMAND ----------


