# Databricks notebook source
# MAGIC %md
# MAGIC # Explore Delta Lake
# MAGIC 
# MAGIC 删除注释
# MAGIC 
# MAGIC 新建注释
# MAGIC 
# MAGIC 在 repos 中修改注释
# MAGIC 
# MAGIC In this notebook, you'll explore how to use Delta Lake in a Databricks Spark cluster.
# MAGIC 
# MAGIC ## Ingest data
# MAGIC 
# MAGIC Use the **&#9656; Run Cell** menu option at the top-right of the following cell to run it and download a data file into the Databricks file system (DBFS).

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/data
# MAGIC rm -r /dbfs/delta
# MAGIC mkdir /dbfs/data
# MAGIC wget -O /dbfs/data/products.csv https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/25/data/products.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs

# COMMAND ----------

# MAGIC %md
# MAGIC Now that you've ingested the data, you can load it into a Spark dataframe from the Databricks file system (DBFS)

# COMMAND ----------

df = spark.read.load('/data/products.csv', format='csv', header=True)
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the file data into a delta table
# MAGIC 
# MAGIC You can persist the data in the dataframe in Delta format using the following code:

# COMMAND ----------

delta_table_path = "/delta/products-delta"



df.write.format("delta").save(delta_table_path)


# COMMAND ----------

# MAGIC %md
# MAGIC The data for a delta lake table is stored in Parquet format. A log file is also created to track modifications made to the data.
# MAGIC 
# MAGIC Use the following shell commands to view the contents of the folder where the delta data has been saved.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/delta/products-delta

# COMMAND ----------

# MAGIC %md
# MAGIC The file data in Delta format can be loaded into a **DeltaTable** object, which you can use to view and update the data in the table. Run the following cell to update the data; reducing the price of product 771 by 10%.

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

deltaTable.

# Update the table (reduce price of product 771 by 10%)
deltaTable.update(
    condition = "ProductID == 771",
    set = { "ListPrice": "ListPrice * 0.9" })

# View the updated data as a dataframe
deltaTable.toDF().show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC The update is persisted to the data in the delta folder, and will be reflected in any new dataframe loaded from that location:

# COMMAND ----------

new_df = spark.read.format("delta").load(delta_table_path)
new_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Data modifications are logged, enabling you to use the *time-travel* capabilities of Delta Lake to view previous versions of the data. For example, use the following code to view the original version of the product data:

# COMMAND ----------

new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
new_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC The log contains a full history of modifications to the data. Use the following code to see a record of the last 10 changes:

# COMMAND ----------

deltaTable.history(10).show(10, False, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog tables
# MAGIC 
# MAGIC So far you've worked with delta tables by loading data from the folder containing the parquet files on which the table is based. You can define *catalog tables* that encapsulate the data and provide a named table entity that you can reference in SQL code. Spark supports two kinds of catalog tables for delta lake:
# MAGIC 
# MAGIC - *External* tables that are defined by the path to the parquet files containing the table data.
# MAGIC - *Managed* tables, that are defined in the Hive metastore for the Spark cluster
# MAGIC 
# MAGIC ### Create an external table
# MAGIC 
# MAGIC The following code creates a new database named **AdventureWorks** and then creates an external tabled named **ProductsExternal** in that database based on the path to the Delta files you defined previously.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS  AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the **Location** property of the new table is the path you specified.
# MAGIC 
# MAGIC You can query the new table by using a SQL `SELECT` statement, like this:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC SELECT * FROM ProductsExternal;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a managed table
# MAGIC 
# MAGIC A *managed* table stores its data files in the Hive metastore for the Spark cluster.
# MAGIC 
# MAGIC Run the following code to create (and then describe) a managed tabled named **ProductsManaged** based on the dataframe you originally loaded from the **products.csv** file (before you updated the price of product 771).

# COMMAND ----------

df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC You did not specify a path for the parquet files used by the table - this is managed for you in the Hive metastore, and shown in the **Location** property in the table description (in the **dbfs:/user/hive/warehouse/** path).
# MAGIC 
# MAGIC From the SQL user's perspective, there's no difference between external and managed tables when it comes to querying them with a `SELECT` statement:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC SELECT * FROM ProductsManaged;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare external and managed tables
# MAGIC 
# MAGIC let's explore the differences between external and managed tables.
# MAGIC 
# MAGIC First, use the following code to list the tables in the **AdventureWorks** database:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a look at the folders on which these tables are based:

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "External table:"
# MAGIC ls /dbfs/delta/products-delta
# MAGIC echo
# MAGIC echo "Managed table:"
# MAGIC ls /dbfs/user/hive/warehouse/adventureworks.db/productsmanaged

# COMMAND ----------

# MAGIC %md
# MAGIC What happens if we use a `DROP` statement to delete these tables from the database?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ProductsExternal;
# MAGIC DROP TABLE IF EXISTS ProductsManaged;
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC The metadata for both tables has beenr emoved from the database; but what about the delta files?

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "External table:"
# MAGIC ls /dbfs/delta/products-delta
# MAGIC echo
# MAGIC echo "Managed table:"
# MAGIC ls /dbfs/user/hive/warehouse/adventureworks.db/productsmanaged

# COMMAND ----------

# MAGIC %md
# MAGIC The files for the managed table are deleted automatically when the table is dropped. However, the files for the external table remain. Dropping an external table only removes the table metadata from the database; it does not delete the data files.
# MAGIC 
# MAGIC You can use the dfollowing code to create a new table in the database that is based on the delta files in the **products-delta** folder:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC CREATE TABLE Products
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/products-delta';

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can query the new table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC 
# MAGIC SELECT * FROM Products;

# COMMAND ----------

# MAGIC %md
# MAGIC Because the table is based on the existing delta files, which include the logged history of changes, it reflects the modifications you previously made to the products data.
# MAGIC 
# MAGIC ## Use delta tables for streaming data
# MAGIC 
# MAGIC Delta lake supports streaming data. Delta tables can be a *sink* or a *source* for data streams created using the Spark Structured Streaming API. In this example, you'll use a delta table as a sink for some streaming data in a simulated internet of things (IoT) scenario.
# MAGIC 
# MAGIC First, let's get some simulated device data in JSON format. Run the following cell to download a JSON file that looks like this:
# MAGIC 
# MAGIC ```json
# MAGIC {"device":"Dev1","status":"ok"}
# MAGIC {"device":"Dev1","status":"ok"}
# MAGIC {"device":"Dev1","status":"ok"}
# MAGIC {"device":"Dev2","status":"error"}
# MAGIC {"device":"Dev1","status":"ok"}
# MAGIC {"device":"Dev1","status":"error"}
# MAGIC {"device":"Dev2","status":"ok"}
# MAGIC {"device":"Dev2","status":"error"}
# MAGIC {"device":"Dev1","status":"ok"}
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/device_stream
# MAGIC mkdir /dbfs/device_stream
# MAGIC wget -O /dbfs/device_stream/devices1.json https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/25/data/devices1.json

# COMMAND ----------

# MAGIC %md
# MAGIC Now you're ready to use Spark Structured Steraming to create a stream based on the folder containing the JSON device data.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a stream that reads data from the folder, using a JSON schema
inputPath = '/device_stream/'
jsonSchema = StructType([
StructField("device", StringType(), False),
StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)
print("Source stream created...")

# COMMAND ----------

# MAGIC %md
# MAGIC Now you'll take the stream of data you're reading from the folder, and perpetually write it to a delta table folder:

# COMMAND ----------

# Write the stream to a delta table
delta_stream_table_path = '/delta/iotdevicedata'
checkpointpath = '/delta/checkpoint'

deltastream = iotstream.writeStream.format("delta").option("checkpointLocation",checkpointpath).start(delta_stream_table_path)

print("Streaming to delta sink...")

# COMMAND ----------

# MAGIC %md
# MAGIC To load the streamed table data, just read the delta table folder source like any other dataframe:

# COMMAND ----------

# Read the data in delta format into a dataframe
df = spark.read.format("delta").load(delta_stream_table_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC You can also create a table based on the streaming delta table folder:

# COMMAND ----------

# create a catalog table based on the streaming sink
spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))

# COMMAND ----------

# MAGIC %md
# MAGIC You can query the table just like any other:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IotDeviceData;

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's add some fresh device data to the stream.

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -O /dbfs/device_stream/devices4.json https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/25/data/devices2.json

# COMMAND ----------

# MAGIC %md
# MAGIC The new JSON data in the device folder is read into the stream and written to the delta folder, where it is reflected in the table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IotDeviceData;

# COMMAND ----------

# MAGIC %md
# MAGIC To stop the stream, use its **stop** method:

# COMMAND ----------

deltastream.stop()
