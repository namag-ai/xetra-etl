// Databricks notebook source
// MAGIC %md
// MAGIC ## Create Tables
// MAGIC This notebook defines the schemas and creates Hive tables for the queries and reports.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS xetra
// MAGIC   LOCATION "/FileStore/tables/xetra"

// COMMAND ----------

// Transaction query table

spark.sql(s"""
  DROP TABLE IF EXISTS xetra.transaction
""")

spark.sql(s"""
  CREATE TABLE xetra.transaction (
    date STRING, 
    security_id INT, 
    time STRING,
    currency STRING,
    start_price FLOAT, 
    max_price FLOAT, 
    min_price FLOAT, 
    end_price FLOAT, 
    traded_volume FLOAT, 
    number_of_trades INT
  )
  USING DELTA 
""")

// COMMAND ----------

// Security query table

spark.sql(s"""
  DROP TABLE IF EXISTS xetra.security
""")

spark.sql(s"""
  CREATE TABLE xetra.security ( 
    security_id INT, 
    isin STRING,
    mnemonic STRING,
    security_type STRING,
    security_desc STRING
  )
  USING DELTA
""")

// COMMAND ----------

// Highest Volume report table

spark.sql(s"""
  DROP TABLE IF EXISTS xetra.highest_volume
""")

spark.sql(s"""
  CREATE TABLE xetra.highest_volume(
    date STRING, 
    security_id INT, 
    implied_volume FLOAT
  )
  USING DELTA 
""")

// COMMAND ----------

// Most Traded report table

spark.sql(s"""
  DROP TABLE IF EXISTS xetra.most_traded
""")

spark.sql(s"""
  CREATE TABLE xetra.most_traded (
    date STRING, 
    security_id INT, 
    trade_amount_mm DOUBLE
  )
  USING DELTA 
""")

// COMMAND ----------

// Biggest Winner report table

spark.sql(s"""
  DROP TABLE IF EXISTS xetra.biggest_winner
""")

spark.sql(s"""
  CREATE TABLE xetra.biggest_winner (
    date STRING, 
    security_id INT, 
    percent_change FLOAT
  )
  USING DELTA 
""")

// COMMAND ----------


