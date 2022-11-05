# Databricks notebook source
import pandas as pd

# COMMAND ----------

import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder\
                .appName("testsession1")\
                .getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

df_pyspark = spark.read.csv("/FileStore/tables/testSM.csv")

# COMMAND ----------

df_pyspark

# COMMAND ----------

df_pyspark.show()

# COMMAND ----------

df = spark.read.option("header","true").csv("/FileStore/tables/testSM.csv")

# COMMAND ----------

df

# COMMAND ----------

df.show()

# COMMAND ----------

df.head()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe()

# COMMAND ----------

df.select(["Order ID", "Sales"]).show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

"Hi"

# COMMAND ----------


