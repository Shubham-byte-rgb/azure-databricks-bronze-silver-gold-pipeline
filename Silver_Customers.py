# Databricks notebook source
# MAGIC %md
# MAGIC ###Import Libraries
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank, desc
import time


# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://bronze@skdatabricksete.dfs.core.windows.net/customers")

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Transformations
# MAGIC
# MAGIC create a seperate column to store domain names  from mail column after splitting.
# MAGIC
# MAGIC ####Sequence
# MAGIC 1. split()
# MAGIC 2. Array Indexing

# COMMAND ----------

df = df.withColumn("domains",split(col('email'),'@')[1])
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation Function 
# MAGIC to check how many customers group by Domain

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers",ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering of the Dataframe based on the Domain

# COMMAND ----------

df_gmail = df.filter(col('domains')== "gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df.filter(col('domains')== "yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df.filter(col('domains')== "hotmail.com")
df_hotmail.display()
time.sleep(5)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a full_name column first_name+last_name
# MAGIC Concate function

# COMMAND ----------

df = df.withColumn("full_name",concat(col('first_name'),lit(' '),col('last_name')))
df.display()
df.drop('first_name','last_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data

# COMMAND ----------

# WRITE
    
print("Before write count =", df.count())

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("sk_databricks_cata.silver.customers_silver")


# DEBUG (IMPORTANT)
print("Rows:", df.count())

df_check = spark.read.table("sk_databricks_cata.silver.customers_silver")

print("Count:", df_check.count())
df_check.show(5)





# COMMAND ----------

# MAGIC %md
# MAGIC ###create Table on Top of it
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS sk_databricks_cata.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists sk_databricks_cata.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@skdatabricksete.dfs.core.windows.net/customers_silver"
# MAGIC

# COMMAND ----------

df.count()

# COMMAND ----------


df_test = spark.read.table("sk_databricks_cata.silver.customers_silver")
print("After write count =", df_test.count())
df_test.show(5)
