# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Regions table 
# MAGIC this perticular file is static file and will not be the part of STAR Schema but still it'll be there.
# MAGIC
# MAGIC its a mapping file.
# MAGIC
# MAGIC regions is a static lookup/reference dataset containing geographical classifications (East, West, North, South).
# MAGIC 	•	In the Silver layer, it is stored after basic cleaning and schema standardization.
# MAGIC 	•	It is promoted to the Gold layer as a curated reference table for BI dashboards, filters, and reporting.
# MAGIC 	•	It also ensures a consistent source of truth and can be joined with future fact tables if required.

# COMMAND ----------

df = spark.read.table("sk_databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data") 

# COMMAND ----------

# MAGIC %md
# MAGIC write the Data
# MAGIC

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@skdatabricksete.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %md
# MAGIC Lets read all the data

# COMMAND ----------

# df = spark.read.format("delta")\
#     .load("abfss://silver@skdatabricksete.dfs.core.windows.net/regions")
# df.display()

df = spark.read.format("delta")\
    .load("abfss://silver@skdatabricksete.dfs.core.windows.net/orders")
df.display()

df = spark.read.format("delta")\
    .load("abfss://silver@skdatabricksete.dfs.core.windows.net/products")
df.display()

df = spark.read.format("delta")\
    .load("abfss://silver@skdatabricksete.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Tabel on top of it

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists sk_databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@skdatabricksete.dfs.core.windows.net/regions"