// Databricks notebook source
// MAGIC %md
// MAGIC ## Run Script
// MAGIC This notebook runs the other Xextra ETL and reporting notebooks.
// MAGIC 
// MAGIC If this was a production ETL pipeline, we could return exit values for testing and monitoring.
// MAGIC 
// MAGIC Notebooks:
// MAGIC 1. FetchData - Grabs the raw Xetra data from the public AWS S3 bucket.
// MAGIC 2. CreateTables - Creates the "xetra" database and initializes schemas.
// MAGIC 3. XetraETL - ETL pipeline to create query and reporting tables.
// MAGIC 4. SummaryReport - Report and visualizations for "biggest winner", "most traded", and "highest volume".

// COMMAND ----------

// MAGIC %run ./FetchData

// COMMAND ----------

// MAGIC %run ./CreateTables

// COMMAND ----------

// MAGIC %run ./XetraETL

// COMMAND ----------

// MAGIC %run ./SummaryReport

// COMMAND ----------


