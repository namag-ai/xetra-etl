# xetra-etl
ETL pipeline for analyzing public stock trading data from the Xetra Exchange using Scala, Spark, and Databricks.

See XetraETL.pdf for a detailed description.

![alt text](https://raw.githubusercontent.com/codywynn/xetra-etl/master/img/Architecture.png)

### Notebooks
- **FetchData:** extracts Xetra data from 2018-02-01 to 2018-02-28 and stores it in the Databricks File System
- **CreateTables:** defines the schemas and creates Hive tables for the queries and reports
- **XetraETL:** pipelines the dataset into "biggest winner", "most traded", and "highest volume" report tables
- **SummaryReport:** provides a detailed report and visualizations of the transformed data
- **RunAllNotebooks:** runs all the notebooks back-to-back

You can view the output of the notebooks through a web browser here:

https://codywynn.github.io/xetra-etl/

### Running On Databricks
Import the XetraETL.dbc file to your Databricks workspace.

Create a cluster with Databricks Runtime 5.4 and start "RunAllNotebooks".

*More info on importing notebooks here:*

https://docs.databricks.com/user-guide/notebooks/notebook-manage.html

### Data Schema
![alt text](https://raw.githubusercontent.com/codywynn/xetra-etl/master/img/XetraSchema.png)
