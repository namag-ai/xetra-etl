// Databricks notebook source
// MAGIC %md
// MAGIC ## Fetch Data
// MAGIC This notebook extracts the Xetra trade data for the dates 2018-02-01 to 2018-02-28, located at s3://deutsche-boerse-xetra-pds/, and stores it in the Databricks File System.
// MAGIC 
// MAGIC For the purpose of this assignment, we use a bash script and the AWS CLI to read data using the "--no-sign-request" option, which takes 10 - 20 minutes to run.
// MAGIC 
// MAGIC If this cluster was configured with an S3 IAM role, it could read directly from the S3 bucket using the spark.read.csv API, which would be much faster.

// COMMAND ----------

// MAGIC %sh 
// MAGIC pip install --upgrade awscli

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir -p /dbfs/FileStore/data/xetra
// MAGIC 
// MAGIC for day in {1..28}
// MAGIC do
// MAGIC   if [ "$day" -lt 10 ]
// MAGIC   then
// MAGIC       DATE="2018-02-0$day"
// MAGIC   else
// MAGIC       DATE="2018-02-$day"
// MAGIC   fi
// MAGIC   
// MAGIC   aws s3 sync s3://deutsche-boerse-xetra-pds/$DATE /dbfs/FileStore/data/xetra --no-sign-request
// MAGIC done
// MAGIC 
// MAGIC ls /dbfs/FileStore/data/xetra
