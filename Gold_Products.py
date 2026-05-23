# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # GOLD LAYER: DIM PRODUCTS 
# MAGIC – DATA PREPARATION & SCD COLUMN SETUP
# MAGIC ##### Batch Processing | SCD Type 2 Implementation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp, lit

# ✅ Read Silver
df = spark.read.table("sk_databricks_cata.silver.products_silver")

print("Input count:", df.count())

# ✅ Apply rules
df_clean = df.filter(
    col("product_id").isNotNull() &
    col("product_name").isNotNull()
)

print("After rules count:", df_clean.count())

# ✅ Add SCD columns
df_new = df_clean \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))





# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # GOLD LAYER: DIM PRODUCTS – SCD TYPE 2 MERGE LOGIC
# MAGIC
# MAGIC #### DESCRIPTION:
# MAGIC #### This cell performs SCD Type 2 logic using Delta Lake MERGE.
# MAGIC #### - Updates existing records by marking them as inactive
# MAGIC #### - Inserts new records as current versions
# MAGIC #### - Maintains historical data in DimProducts table
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

table_name = "sk_databricks_cata.gold.DimProducts"

try:
    target = DeltaTable.forName(spark, table_name)

    print("MERGE mode")

    target.alias("trg").merge(
        df_new.alias("src"),
        "trg.product_id = src.product_id AND trg.is_current = true"
    ).whenMatchedUpdate(set={
        "is_current": lit(False),
        "effective_end_date": current_timestamp()
    }).whenNotMatchedInsertAll().execute()

except:
    print("FIRST LOAD")

    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

