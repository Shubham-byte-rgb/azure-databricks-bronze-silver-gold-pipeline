# Databricks notebook source
# MAGIC %md
# MAGIC Import the Libraries

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
# MAGIC

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://bronze@skdatabricksete.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create temp view
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function sk_databricks_cata.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price *0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,price as original_price, sk_databricks_cata.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

# MAGIC %md
# MAGIC create function on to of Dataframe in sql

# COMMAND ----------

df = df.withColumn("discounted_price",expr("sk_databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create the function Using Python on top of brand
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function sk_databricks_cata.bronze.Upper_func(p_brand STRING)
# MAGIC returns string
# MAGIC Language PYTHON
# MAGIC as 
# MAGIC $$
# MAGIC
# MAGIC    return p_brand.upper()
# MAGIC
# MAGIC
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,brand,sk_databricks_cata.bronze.Upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write the DF

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@skdatabricksete.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###create Table on Top of it
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists sk_databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@skdatabricksete.dfs.core.windows.net/products"