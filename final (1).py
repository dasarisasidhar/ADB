# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

from pyspark.sql.functions import col, explode, sum, desc, struct, when

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------

spark = SparkSession.builder.appName("final").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

#reading the csv file
df = spark.read.csv("/FileStore/tables/testSM.csv", header= True, inferSchema = True)

# COMMAND ----------

df.show()

# COMMAND ----------

#creating new data frame with data
df1 = spark.createDataFrame(data=[(1,2),(2,4),(5,6)],schema=("a","b"))

# COMMAND ----------

df1.show()

# COMMAND ----------

#creating pandas df
pandas_df = pd.DataFrame(data=[(1,2),(2,4),(5,6)],columns=("a","b"))

# COMMAND ----------

pandas_df

# COMMAND ----------

# converting pandas df to pyspark df
sparkdf1 = spark.createDataFrame(pandas_df)

# COMMAND ----------

sparkdf1.show()

# COMMAND ----------

#all functions cheat sheet
#in pyspark df != Df it is case censitive
#df.show() or df.show(n = 2) --> to display the values if 2/value is given it will retriev top 2 records
#show contains 3 perameters by default n=20, truncate=True, vertical=False
#pf.printSchema() --> to print the schema
#df.withColumn("Quantity1", df["Quantity"].cast(IntegerType())) --> to change the datatype of column ->cast
#df.withColumn("newcolname", values)
#df.select('Quantity').printSchema() -> to get single column schema
#df.select('Quantity').dtypes-> to get single dtype
#df.select(['a',b]) to select the perticular columns from df
#df.collect is used to retrive the data from rdd and dataframe to list dtype
#df.count() returns the no of records in df
#df.describe('Sales').show() or df.describe(['Sales', "Quantity"]).show() - describes each int column stats
#df.orderBy(df.Sales).show()
#df.select('OrderID','Sales','Quantity').orderBy(df.Sales).show()
#df.select('Order ID','Sales','Quantity').orderBy(df.Sales, ascending = False).show() #by default ascending is True. Note: desending = True/False is not working
#df.groupby('Quantity').count().show()
#df.distinct().show() --> shows all the distinct records
#df.distinct().count()--> will give count
#spd.select(['a']).distinct().show() --> distinct of certain column
#spd.groupby('a', 'b', 'c').count().filter("count > 1").show() -> this will first groupby and counts then it will display all the records having count more than 1;
#spd.sort(desc('a')).show() , desc should be imported from spark.sql.functions
#df.first()
#df.first()[0]


# COMMAND ----------

#show()
#printSchema()
#withColumn()
#withColumnRenamed()
#select()
#collect()
#count()
#describe()
#orderby()
#groupby()
#distinct()
#filter()
#sort()
#alias()
#first()


# COMMAND ----------

#reading excel data to pandas and converting to pyspark
pDF = pd.read_excel("/FileStore/tables/Tesla_sales_data.xlsx")
sDF = spark.createDataFrame("pDF")
#this process will not work because pandas looks for local file but here is DBFS file

# COMMAND ----------

#reading excel - /FileStore/tables/Tesla_sales_data.xlsx
sparkDF = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/tables/Tesla_sales_data.xlsx")

# this reading will not work because of bo package is available with com.crealytics.spark.excel in maven for spark 3.3.1
#in order use the excel in pyspark first setup --> cluster --> libraries --> install new --> maven -->search package -->change from spark to maven package --> search spark-excel --> find suitable package for current spark version and install.


# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#ps.read_excel("dbfs:/FileStore/tables/Tesla_sales_data.xlsx") needs all string or byte like obj

# COMMAND ----------

#df.withColumn("Quantity", df["Quantity"].cast(IntegerType())) --> with this you can cast the Quantity column to int and store in same column
df = df.withColumn("Quantity1", df["Quantity"].cast(IntegerType())) #cast the Quantity and store in new col

# COMMAND ----------

df.show(2)

# COMMAND ----------

df[df["Quantity"].cast(IntegerType())]

# COMMAND ----------

	
df.select('Quantity').printSchema()
df.select('Quantity').dtypes

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.select(['Quantity','Sales']).show()

# COMMAND ----------

x = df.collect()
type(x)

# COMMAND ----------

df.count()

# COMMAND ----------

df.describe(['Sales','Quantity']).show()

# COMMAND ----------

df.orderBy(df.Profit).show()

# COMMAND ----------

df.select('Order ID','Sales','Quantity').orderBy(df.Sales, ascending = True).show()

# COMMAND ----------

#df.select('Order ID','Sales','Quantity').orderBy(df.Sales, descending = False).show() descending is not working

# COMMAND ----------

df.groupby('Ship Mode').count().show()

# COMMAND ----------

#Removing duplicates in df

# COMMAND ----------

Spd= spark.createDataFrame([(1,2,3),(4,5,6),(1,2,3)], ("a", "b", "c"))

# COMMAND ----------

Spd.show()

# COMMAND ----------

spd= spark.createDataFrame([(1,2,3),(4,5,6),(1,2,3),(4,5,6),(7,8,9)], ("a", "b", "c"))

# COMMAND ----------

spd.show()

# COMMAND ----------

spd.select(['a']).distinct().show()

# COMMAND ----------

spd.columns

# COMMAND ----------

spd.groupby('a', 'b', 'c').count().filter("count > 1").show()

# COMMAND ----------

#

# COMMAND ----------

#

# COMMAND ----------

spd.show()

# COMMAND ----------

spd.dropDuplicates().show()

# COMMAND ----------

spd.distinct().show()

# COMMAND ----------

spd.dropDuplicates(["a", "b"]).show()

# COMMAND ----------

spd.select(["a", "b"]).distinct().show() 

# COMMAND ----------

spd.show()

# COMMAND ----------

#differecnce b/w dropduplicate vs distinct
#spd.select(["a", "b"]).distinct().show()  != spd.dropDuplicates(["a", "b"]).show() but spd.distinct() == spd.dropDuplicates()
#spd.select(["a", "b"]).distinct().show()  first selects the 2 columns and displays a,b by removing all duplicates on a,b
#spd.dropDuplicates(["a", "b"]).show() #displays a,b,c by removing all the duplicate records of a,b 

# COMMAND ----------

spd.distinct().count()

# COMMAND ----------

spd.orderBy("a", ascending = False).show()

# COMMAND ----------

spd.sort(desc('a')).show()

# COMMAND ----------

df.first()

# COMMAND ----------

#creating the empty data frame
spark.createDataFrame(data = "", schema = "")

# COMMAND ----------

#create empty df pandas
pd.DataFrame()

# COMMAND ----------

#creating empty RDD
spark.sparkContext.emptyRDD()

# COMMAND ----------

#Creates Empty RDD using parallelize
rdd2= spark.sparkContext.parallelize([])

# COMMAND ----------

#creating schema --> Structfield contains -> name, type, nullable or not
#by default nullable is True
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])


# COMMAND ----------

#nested structure schema
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

# COMMAND ----------

#df.schema.fieldNames.contains("Sales")
#df.schema.contains(StructField("Sales",IntegerType)

# COMMAND ----------

#will not Display full column contents
df.show(2, truncate = True)

# COMMAND ----------

#Display full column contents
df.show(2, truncate = False)

# COMMAND ----------

# this will show 3 records verticaly with each row lenght is truncated to 25
df.show(3,truncate=25,vertical=True)

# COMMAND ----------

len("Plantronics CS510 - Ov...")

# COMMAND ----------

# row class and col class

# COMMAND ----------

df.columns

# COMMAND ----------

#selecting all columns using select

# COMMAND ----------

df.select([col for col in df.columns]).show()


# COMMAND ----------

df.select("*").show()

# COMMAND ----------

#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

# COMMAND ----------

df.columns[:3]

# COMMAND ----------

#for nested schema name -> firstname, lastname, middle name user below select
df2.select("name.firstname","name.lastname").show(truncate=False)
df2.select("name.*").show(truncate=False)

# COMMAND ----------

#deptDF.collect() returns Array of Row type.
#deptDF.collect()[0] returns the first element in an array (1st row).
#deptDF.collect[0][0] returns the value of the first row & first column.

