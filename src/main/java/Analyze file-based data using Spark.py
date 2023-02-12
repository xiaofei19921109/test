# Databricks notebook source
# MAGIC %md
# MAGIC # Explore data in a dataframe
# MAGIC 
# MAGIC In this notebook, you'll use Spark in Azure Databricks to explore data in files. One of the core ways in which you work with data in Spark is to load data into a **Dataframe** object, and then query, filter, and manipulate the dataframe to explore the data it contains.
# MAGIC 
# MAGIC ## Ingest data
# MAGIC 
# MAGIC Use the **&#9656; Run Cell** menu option at the top-right of the following cell to run it and download data files into the Databricks file system (DBFS).

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/data
# MAGIC mkdir /dbfs/data
# MAGIC wget -O /dbfs/data/2019.csv https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/24/data/2019.csv
# MAGIC wget -O /dbfs/data/2020.csv https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/24/data/2020.csv
# MAGIC wget -O /dbfs/data/2021.csv https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/24/data/2021.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query data in files
# MAGIC 
# MAGIC The previous cell downloaded three comma-separated values (CSV) files to the **data** folder in the DBFS storage for your workspace.
# MAGIC 
# MAGIC Run the following cell to load the data from the file and view the first 100 rows.

# COMMAND ----------

df = spark.read.load('data/*.csv', format='csv')
display(df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC The data in the file relates to sales orders, but doesn't include the column headers or information about the data types. To make more sense of the data, you can define a *schema* for the dataframe.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
 ])

df = spark.read.load('/data/*.csv', format='csv', schema=orderSchema)
display(df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC This time the data includes the column headers.
# MAGIC 
# MAGIC To verify that the appropriate data types have been defined, you van view the schema of the dataframe.

# COMMAND ----------

 df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze data in a dataframe
# MAGIC The dataframe object in Spark is similar to a *Pandas* dataframe in Python, and includes a wide range of functions that you can use to manipulate, filter, group, and otherwise analyze the data it contains.
# MAGIC 
# MAGIC ## Filter a dataframe
# MAGIC 
# MAGIC Run the following cell to:
# MAGIC 
# MAGIC - Filter the columns of the sales orders dataframe to include only the customer name and email address.
# MAGIC - Count the total number of order records
# MAGIC - Count the number of distinct customers
# MAGIC - Display the customers

# COMMAND ----------

customers = df['CustomerName', 'Email']
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Observe the following details:
# MAGIC 
# MAGIC - When you perform an operation on a dataframe, the result is a new dataframe (in this case, a new customers dataframe is created by selecting a specific subset of columns from the df dataframe)
# MAGIC - Dataframes provide functions such as count and distinct that can be used to summarize and filter the data they contain.
# MAGIC - The `dataframe['Field1', 'Field2', ...]` syntax is a shorthand way of defining a subset of column. You can also use **select** method, so the first line of the code above could be written as `customers = df.select("CustomerName", "Email")`
# MAGIC 
# MAGIC Now let's apply a filter to include only the customers who have placed an order for a specific product:

# COMMAND ----------

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Note that you can “chain” multiple functions together so that the output of one function becomes the input for the next - in this case, the dataframe created by the select method is the source dataframe for the where method that is used to apply filtering criteria.
# MAGIC 
# MAGIC ### Aggregate and group data in a dataframe
# MAGIC 
# MAGIC Run the following cell to aggregate and group the order data.

# COMMAND ----------

productSales = df.select("Item", "Quantity").groupBy("Item").sum()
display(productSales)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the results show the sum of order quantities grouped by product. The **groupBy** method groups the rows by *Item*, and the subsequent **sum** aggregate function is applied to all of the remaining numeric columns (in this case, *Quantity*)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try another aggregation.

# COMMAND ----------

yearlySales = df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
display(yearlySales)

# COMMAND ----------

# MAGIC %md
# MAGIC This time the results show the number of sales orders per year. Note that the select method includes a SQL **year** function to extract the year component of the *OrderDate* field, and then an **alias** method is used to assign a columm name to the extracted year value. The data is then grouped by the derived *Year* column and the **count** of rows in each group is calculated before finally the **orderBy** method is used to sort the resulting dataframe.
# MAGIC 
# MAGIC ## Query data using Spark SQL
# MAGIC 
# MAGIC As you’ve seen, the native methods of the dataframe object enable you to query and analyze data quite effectively. However, many data analysts are more comfortable working with SQL syntax. Spark SQL is a SQL language API in Spark that you can use to run SQL statements, or even persist data in relational tables.
# MAGIC 
# MAGIC ### Use Spark SQL in PySpark code
# MAGIC 
# MAGIC The default language in Azure Synapse Studio notebooks is *PySpark*, which is a Spark-based Python runtime. Within this runtime, you can use the **spark.sql** library to embed Spark SQL syntax within your Python code, and work with SQL constructs such as tables and views.

# COMMAND ----------

df.createOrReplaceTempView("salesorders")

spark_df = spark.sql("SELECT * FROM salesorders")
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Observe that:
# MAGIC 
# MAGIC - The code persists the data in the **df** dataframe as a temporary view named **salesorders**. Spark SQL supports the use of temporary views or persisted tables as sources for SQL queries.
# MAGIC - The **spark.sql** method is then used to run a SQL query against the **salesorders** view.
# MAGIC - The results of the query are stored in a dataframe.
# MAGIC 
# MAGIC ### Run SQL code in a cell
# MAGIC 
# MAGIC While it’s useful to be able to embed SQL statements into a cell containing PySpark code, data analysts often just want to work directly in SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC     SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
# MAGIC FROM salesorders
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC ORDER BY OrderYear;

# COMMAND ----------

# MAGIC %md
# MAGIC Observe that:
# MAGIC 
# MAGIC - The ``%sql` line at the beginning of the cell (called a magic) indicates that the Spark SQL language runtime should be used to run the code in this cell instead of PySpark.
# MAGIC - The SQL code references the **salesorder** view that you created previously using PySpark.
# MAGIC - The output from the SQL query is automatically displayed as the result under the cell.
# MAGIC 
# MAGIC > **Note**: For more information about Spark SQL and dataframes, see the [Spark SQL documentation](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html).
# MAGIC 
# MAGIC ## Visualize data with Spark
# MAGIC 
# MAGIC A picture is proverbially worth a thousand words, and a chart is often better than a thousand rows of data. While notebooks in Azure Databricks include support for visualizing data from a dataframe or Spark SQL query, it is not designed for comprehensive charting. However, you can use Python graphics libraries like matplotlib and seaborn to create charts from data in dataframes.
# MAGIC 
# MAGIC ### View results as a visualization
# MAGIC 
# MAGIC Run the following cell to query the **salesorders** table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM salesorders

# COMMAND ----------

# MAGIC %md
# MAGIC Above the table of results, select **+** and then select **Visualization** to view the visualization editor, and then apply the following options:
# MAGIC     - **Visualization type**: Bar
# MAGIC     - **X Column**: Item
# MAGIC     - **Y Column**: *Add a new column and select* **Quantity**. *Apply the* **Sum** *aggregation*.
# MAGIC     
# MAGIC   Save the visualization and then re-run the code cell to view the resulting chart in the notebook.
# MAGIC 
# MAGIC ### Get started with matplotlib
# MAGIC 
# MAGIC You can get mroe control over data visualizations by using graphics libraries.
# MAGIC 
# MAGIC Run the following cell to retrieve some sales order data into a dataframe.

# COMMAND ----------

sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
             SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
         FROM salesorders \
         GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
         ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_spark.show()

# COMMAND ----------

# MAGIC %md
# MAGIC To visualize the data as a chart, we’ll start by using the matplotlib Python library. This library is the core plotting library on which many others are based, and provides a great deal of flexibility in creating charts.

# COMMAND ----------

from matplotlib import pyplot as plt

# matplotlib requires a Pandas dataframe, not a Spark one
df_sales = df_spark.toPandas()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

# Display the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Review the results, which consist of a column chart with the total gross revenue for each year. Note the following features of the code used to produce this chart:
# MAGIC 
# MAGIC - The **matplotlib** library requires a Pandas dataframe, so you need to convert the Spark dataframe returned by the Spark SQL query to this format.
# MAGIC - At the core of the **matplotlib** library is the **pyplot** object. This is the foundation for most plotting functionality.
# MAGIC - The default settings result in a usable chart, but there’s considerable scope to customize it, as you'll see by running the following cell.

# COMMAND ----------

# Clear the plot area
plt.clf()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

# Show the figure
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC A plot is technically contained with a **Figure**. In the previous examples, the figure was created implicitly for you; but you can create it explicitly.

# COMMAND ----------

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(8,3))

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

# Show the figure
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC A figure can contain multiple subplots, each on its own axis.

# COMMAND ----------

# Clear the plot area
plt.clf()

# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))

# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')

# Create a pie chart of yearly order counts on the second axis
yearly_counts = df_sales['OrderYear'].value_counts()
ax[1].pie(yearly_counts)
ax[1].set_title('Orders per Year')
ax[1].legend(yearly_counts.keys().tolist())

# Add a title to the Figure
fig.suptitle('Sales Data')

# Show the figure
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **Note**: To learn more about plotting with matplotlib, see the [matplotlib documentation](https://matplotlib.org/).
# MAGIC 
# MAGIC ### Use the seaborn library
# MAGIC 
# MAGIC While **matplotlib** enables you to create complex charts of multiple types, it can require some complex code to achieve the best results. For this reason, over the years, many new libraries have been built on the base of matplotlib to abstract its complexity and enhance its capabilities. One such library is **seaborn**.

# COMMAND ----------

import seaborn as sns

# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The **seaborn** library makes it simpler to create complex plots of statistical data, and enables you to control the visual theme for consistent data visualizations.

# COMMAND ----------

# Clear the plot area
plt.clf()

# Set the visual theme for seaborn
sns.set_theme(style="whitegrid")

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Both **matplotlib** and **seaborn** support multiple charts types. For example, run the following cell to view the yearly sales totals as a line chart.

# COMMAND ----------

# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **Note**: To learn more about plotting with seaborn, see the [seaborn documentation](https://seaborn.pydata.org/index.html).
# MAGIC 
# MAGIC 
# MAGIC In this notebook, you've explored some basic techniques for using Spark to explore data in files. To learn more about working with Dataframes in Azure Databricks using PySpark, see [Introduction to DataFrames - Python](https://docs.microsoft.com/azure/databricks/spark/latest/dataframes-datasets/introduction-to-dataframes-python) in the Azure Databricks documentation.
