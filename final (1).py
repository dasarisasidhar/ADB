# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.window import Window


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

from pyspark.sql.functions import col, explode, sum, desc, struct, when, row_number, rank, dense_rank, percent_rank

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
#df.withColumn("newcolname", values) -> inplace is true, no need to assign
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
#df.first()[0][0] or df.first()[0]
#explain()
#filter() --> supports ~ operator
#drop()
#dropDuplicates()
#df.distinct()
#INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN -->inner*, outer = full = fullouter, left = leftouter, right = rightouter, leftsemi, leftanti, rightsemi, rightanti
#createOrReplaceTempView

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
#filter()
#explain()
#drop()
#dropDuplicates()
#distinct

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


# COMMAND ----------

# filter() for rdd/df == where for sql

#PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or SQL expression, you can also use where() clause instead of the filter() #if you are coming from an SQL background, both these functions operate exactly the same.



# COMMAND ----------

df

# COMMAND ----------

#filter()
df.filter(df.Sales >= 5000).orderBy("Sales",ascending = False).show()

# COMMAND ----------

df.filter(df.Sales >= 5000).orderBy("Sales",ascending = False).explain()

# COMMAND ----------

df.filter(~(df.Sales >= 5000)).orderBy("Sales",ascending = True).show()

# COMMAND ----------

#Using SQL Expression
df.filter("gender == 'M'").show()
#For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

# COMMAND ----------

#Filter multiple condition
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)  

# COMMAND ----------

#Filter IS IN List values
li=["OH","CA","DE"]
df.filter(df.state.isin(li))

# COMMAND ----------

# Using startswith
df.filter(df.state.startswith("N")).show()
#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()
# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()

# COMMAND ----------

#filter and array_contains
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)   

# COMMAND ----------

#Struct condition
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 

# COMMAND ----------

#drop duplicates
dropDisDF = df.dropDuplicates(["Sales", "Quantity"])

# COMMAND ----------

dropDisDF.count()

# COMMAND ----------

dropDisDF = df.select("Sales", "Quantity").distinct()

# COMMAND ----------

dropDisDF.count()

# COMMAND ----------

df.count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df.dropDuplicates().count()

# COMMAND ----------

#INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN

# COMMAND ----------

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

# COMMAND ----------

df = spark.createDataFrame(emp, empColumns)

# COMMAND ----------

df.withColumn("superior_emp_id", df["superior_emp_id"].cast(IntegerType())).withColumn("year_joined", df["year_joined"].cast(IntegerType())).withColumn("salary", df["salary"].cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(3)

# COMMAND ----------

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]

# COMMAND ----------

dept_df = spark.createDataFrame(dept, deptColumns)

# COMMAND ----------

dept_df.withColumn("dept_id", dept_df["dept_id"].cast(IntegerType()))

# COMMAND ----------

dept_df.printSchema()

# COMMAND ----------

df.join(dept_df,df.emp_dept_id ==  dept_df.dept_id,"inner") \
     .show(truncate=False)

# COMMAND ----------

df.join(dept_df,df.emp_dept_id ==  dept_df.dept_id,"inner").select("emp_id", "name", "dept_name").show()

# COMMAND ----------

df.join(dept_df, df.emp_dept_id ==  dept_df.dept_id, "right").show()

# COMMAND ----------

df.join(dept_df, df.emp_dept_id ==  dept_df.dept_id, "left").show()

# COMMAND ----------

dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "right").show()

# COMMAND ----------

#Outer a.k.a full, fullouter join returns all rows from both datasets, where join expression doesn’t match it returns null on respective record columns
dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "outer").show()

# COMMAND ----------

dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "full").show()

# COMMAND ----------

dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "fullouter").show()

# COMMAND ----------

dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "left").show()

# COMMAND ----------

#Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match
dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "leftouter").show()

# COMMAND ----------

#n returns all columns from the left dataset and ignores all columns from the right dataset
dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "leftsemi").show()
#matched columns

# COMMAND ----------

#returns all columns from the left dataset and ignores all columns from the right dataset
dept_df.join(df, df.emp_dept_id ==  dept_df.dept_id, "leftanti").show()
#notmached columns

# COMMAND ----------

#slef join, use inner
empDF = df
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

# COMMAND ----------

empDF.createOrReplaceTempView("EMP")
dept_df.createOrReplaceTempView("DEPT")

# COMMAND ----------

#using sql directly 

df2 = spark.sql("select * from EMP e join DEPT d on e.emp_dept_id = d.dept_id")

# COMMAND ----------

df2.show()

# COMMAND ----------

#joining multiple df
df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")

# COMMAND ----------

df.write.csv("dbfs:/FileStore/df/today_report.csv")


# COMMAND ----------

#union and unionall - union() method of the DataFrame is used to combine two DataFrame’s of the same structure/schema.
#If schemas are not the same it returns an error.
df1.union(df2)
df1.unionAll(df2)
df.union(df2).distinct() #--> combines to df and gives only single ->removes duplicates after union
    

# COMMAND ----------

#The difference between unionByName() function and union() is that this function
#resolves columns by name (not by position). In other words, unionByName() is used to merge two DataFrame’s by column names instead of by position.
new_df1.unionByName(new_df2)


# COMMAND ----------

""" Converting function to UDF """
convertUDF = udf(lambda z: convertCase(z),StringType())

# COMMAND ----------

#RDD map() transformation is used to apply any complex operations like adding a column, updating a column, transforming the data e.t.c, the output of map transformations would #always have the same number of records as input

# COMMAND ----------

#only on RDD - map, flatmap
#Note1: DataFrame doesn’t have map() transformation to use with DataFrame hence you need to DataFrame to RDD first.
#Note2: If you have a heavy initialization use PySpark mapPartitions() transformation instead of map(), as with mapPartitions() heavy initialization executes only once for each #partition instead of every record.
#PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns) after applying the function on every element and returns a new PySpark RDD/DataFrame. In this article, you will learn the syntax and usage of the PySpark flatMap() with an example.

# COMMAND ----------

#on df - foreach(), foreachPartitions()
#PySpark also provides foreach() & foreachPartitions() actions to loop/iterate through each Row in a DataFrame but these two returns nothing, In this article, I will explain how to use these methods to get DataFrame column values and process.

# Foreach example
def f(x): print(x)
df.foreach(f)

# Another example
df.foreach(lambda x: 
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    ) 

# COMMAND ----------

#sample -> is a mechanism to get random sample records from the dataset,
df.sample(withReplacement, fraction, seed=None)
#fraction – Fraction of rows to generate, range [0.0, 1.0]. Note that it doesn’t guarantee to provide the exact number of the fraction of records.

#seed – Seed for sampling (default a random seed). Used to reproduce the same random sampling.
#withReplacement – Sample with replacement or not (default False).
df.sampleBy(col, fractions, seed=None)
df2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect()


# COMMAND ----------

fillna(value, subset=None)
fill(value, subset=None)
#Replace 0 for null for all integer columns
df.na.fill(value=0).show()
#Replace 0 for null on only population column 
df.na.fill(value=0,subset=["population"]).show()

# COMMAND ----------

a = [(1,1), (1,1)]
col = ("a", "b")

df1 = spark.createDataFrame(a,col)
df2 = spark.createDataFrame(a,col)


# COMMAND ----------

df1.join(df2, df1.a == df2.a, "right").show()

# COMMAND ----------

data = [('Carine', 'Texas', 'carine@javatpoint.com'),  
('Carine', 'Texas', 'carine@javatpoint.com'),  
('Peter', 'New York', 'peter@javatpoint.com'),  
('Janine ', 'Florida', 'janine@javatpoint.com'),  
('Janine ', 'Florida', 'janine@javatpoint.com'),  
('Jonas ', 'Atlanta', 'jonas@javatpoint.com'),  
('Jean', 'California', 'jean@javatpoint.com'),  
('Jean', 'California', 'jean@javatpoint.com'),  
('Mark ', 'Florida', 'mark@javatpoint.com'),  
('Roland', 'Alabama', 'roland@javatpoint.com'),  
('Roland', 'Alabama', 'roland@javatpoint.com'),  
('Julie', 'Texas', 'julie@javatpoint.com'),  
('Shane', 'New York', 'shane@javatpoint.com'),  
('Susan', 'Arizona', 'susan@javatpoint.com'),  
('Susan', 'Arizona', 'susan@javatpoint.com')]

# COMMAND ----------

schema = ("fname", "lname", "email")

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.select(*schema).groupby(*schema).count().filter('count(*)>1').show()
#==
#select *, count(*) from table groupby(allColumnName) having(count(*)>1)

# COMMAND ----------

#PySpark Window Functions - rank, percent_rank, dense_rank, row number 
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100), \
              ("Saif", "Sales", 100)\
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

windowSpec  = Window.partitionBy("department").orderBy("salary") #this will do partitionby depatment and order by salary, windows fun don't have sort

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

#this will rank
#desc wont work directly

# COMMAND ----------

df = df.orderBy('salary', ascending = False)

# COMMAND ----------

windowSpec  = Window.partitionBy("department").orderBy("salary")
df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# COMMAND ----------

df.withColumn("rank", rank().over(windowSpec)).show()

# COMMAND ----------

#dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

# COMMAND ----------

#percent_rank - % of total recodes in each partitions
df.withColumn("percent_rank", percent_rank().over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import ntile #divides total records of each partition by ntile number
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import cume_dist    
df.withColumn("cume_dist",cume_dist().over(windowSpec)) \
   .show()
