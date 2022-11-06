# Databricks notebook source

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

df.show()

# COMMAND ----------

data2

# COMMAND ----------

rdd = spark.sparkContext.parallelize(data2)

# COMMAND ----------

rdd

# COMMAND ----------

df = rdd.toDF()

# COMMAND ----------

df

# COMMAND ----------

df.show()

# COMMAND ----------

columns = ["Fname","Mname","Lname", "id", "Gender", "Total"]
df1 = rdd.toDF(columns)

# COMMAND ----------

df1.show()

# COMMAND ----------

len(columns)

# COMMAND ----------

spark.createDataFrame(rdd).toDF(columns)
#pass values, passing list of elements is considered as single values

# COMMAND ----------

df2 = spark.createDataFrame(rdd).toDF("Fname","Mname","Lname", "id", "Gender", "Total")

# COMMAND ----------

df2.show()

# COMMAND ----------

df3 = spark.read.csv("/FileStore/tables/tips.csv")

# COMMAND ----------

df3

# COMMAND ----------

#df=spark.createDataFrame(data,columns) syntax to create df

# COMMAND ----------

df3 = spark.sql("SELECT * FROM tips_csv")

# COMMAND ----------

df3

# COMMAND ----------

df3.show()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

#rename columns of DF
df3.show()

# COMMAND ----------

df3.withColumnRenamed("sex","gender")

# COMMAND ----------

df3

# COMMAND ----------

df3 = df3.withColumnRenamed("sex","gender")

# COMMAND ----------

df3

# COMMAND ----------

#multiple column rename

# COMMAND ----------

df3 = df3.withColumnRenamed("gender","sex") \
    .withColumnRenamed("tip","tips")

# COMMAND ----------

df3.columns

# COMMAND ----------

df3 = df3.toDF('total_bill', 'tip', 'gender', 'smoker', 'day', 'time', 'size')

# COMMAND ----------

df3.show()

# COMMAND ----------

#Change DataType using PySpark withColumn()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df3.withColumn("total_bill",col("total_bill").cast("Integer")).show()

# COMMAND ----------

#Update The Value of an Existing Column
df3.withColumn("total_bill",col("total_bill")*10).show()

# COMMAND ----------

#Create a Column from an Existing
df3.withColumn("CopiedColumn",col("total_bill")* -1).show()

# COMMAND ----------

#Drop Column From PySpark DataFrame
df3.drop("size").show() 

# COMMAND ----------

#In PySpark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.



# COMMAND ----------



states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)

# COMMAND ----------


states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

def state_convert(code):
    return broadcastStates.value[code]

result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# COMMAND ----------

#The PySpark Accumulator is a shared variable that is used with RDD and DataFrame to perform sum and counter operations similar to Map-reduce counters. These variables are shared by all executors to update and add information through aggregation or computative operations.



# COMMAND ----------

#Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to update and updates from the workers get propagated automatically to the driver program. But, only the driver program is allowed to access the Accumulator variable using the value property.



# COMMAND ----------

accumCount=spark.sparkContext.accumulator(0)
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)

# COMMAND ----------

accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)

accuSum=spark.sparkContext.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
rdd.foreach(countFun)
print(accuSum.value)

accumCount=spark.sparkContext.accumulator(0)
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)

# COMMAND ----------

df3.drop(how='any', thresh=None, subset=None)

# COMMAND ----------

df3.na.drop().show()

# COMMAND ----------

df3.na.drop("any").show()

# COMMAND ----------

df3.na.drop("all")

# COMMAND ----------

df3.na.drop("all").show()

# COMMAND ----------

df3.na.drop(subset=["gender","tip"])

# COMMAND ----------

df3.dropna()

# COMMAND ----------


df3.sort("tip", "gender").show()

# COMMAND ----------

df3.sort(col("tip"),col("gender")).show(truncate=False)

# COMMAND ----------

df3.orderBy("tip","gender").show()

# COMMAND ----------

df3.orderBy(col("tip"),col("gender")).show()

# COMMAND ----------

df3.sort(df3.tip.asc(),df3.gender.asc()).show(truncate=False)

# COMMAND ----------

df3.sort(col("tip").asc(),col("gender").asc()).show(truncate=False)
df3.orderBy(col("tip").asc(),col("gender").asc()).show(truncate=False)


# COMMAND ----------

# use desc to desending order
df3.sort(df3.tip.desc()).show()

# COMMAND ----------


