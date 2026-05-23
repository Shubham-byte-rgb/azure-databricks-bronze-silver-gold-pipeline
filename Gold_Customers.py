# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

spark.sql("USE CATALOG sk_databricks_cata")
spark.sql("USE SCHEMA silver")
spark.sql("SELECT current_catalog(), current_schema()").show()

# COMMAND ----------

try:
    init_load_flag = int(dbutils.widgets.get("init_load_flag"))
except:
    init_load_flag = 1  # default value

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a flag
# MAGIC real-world solution for SCD Type - 1 Dim
# MAGIC Initial Load / Full Load + Incremental load within one notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading from source

# COMMAND ----------

spark.sql("SHOW TABLES IN sk_databricks_cata.silver").show(truncate=False)

# COMMAND ----------


df = spark.read.table("sk_databricks_cata.silver.customers_silver")
df.show(5)


# COMMAND ----------


df = spark.read.table("sk_databricks_cata.silver.customers_silver")
df.show(5)



# COMMAND ----------


f = spark.read.table("sk_databricks_cata.silver.customers_silver")
df.show(5)


# COMMAND ----------

#spark.read.table("sk_databricks_cata.silver.customers_silver").show()

# COMMAND ----------

df = spark.sql("select * from sk_databricks_cata.silver.customers_silver")

#df = spark.read.table("sk_databricks_cata.silver.customers_silver")

#df = spark.read.table("customers_silver")
df.show(5)

#df = spark.read.table("sk_databricks_cata.silver.customers_silver")

# COMMAND ----------

spark.sql("SELECT current_catalog(), current_schema()").show()

# COMMAND ----------

display(spark.table("silver.customers_silver"))


# COMMAND ----------

df.show()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Removing Duplicates

# COMMAND ----------

df = df.dropDuplicates(subset=["customer_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC **Dividing New vs Old records**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey,customer_id,create_date,update_date 
                       from sk_databricks_cata.gold.DimCustomers''')
    

else:

    df_old = spark.sql('''select 0 DimCustomerKey,  0 customer_id,  0  create_date, 0 update_date
                      From sk_databricks_cata.silver.customers_silver  where 1=0''')


# COMMAND ----------

df_old.display()
df_old.orderBy("DimCustomerKey").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming Columns of df_old

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey","old_DimCustomerKey")\
    .withColumnRenamed("customer_id","old_customer_id")\
    .withColumnRenamed("create_date","old_create_date")\
    .withColumnRenamed("update_date","old_update_date")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Joins with the new records**

# COMMAND ----------

df_join = df.join(df_old,df['customer_id'] == df_old['old_customer_id'],'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# Dropping all the column which are not required. 

df_old = df_old.drop("old_customer_id","old_update_date")

# Renaming "old_DimCustomerKey" to "DimCustomerKey"" column

df_old = df_old.withColumnRenamed("old_DimCustomerKey","DimCustomerKey")

# Renaming "old_create_date" to "create-date" column

df_old = df_old.withColumnRenamed("old_create_date","create_date")

df_old = df_old.withColumn("create_date",to_timestamp(col("create_date")))

# Recreating "update_date" column with the current timestamp

df_old = df_old.withColumn("update_date",current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

# Dropping all the column which are not required. 

df_new = df_new.drop("old_DimCustomerKey","old_customer_id","old_update_date","old_create_date")

# Recreating "update_date"and "create_date" column with the current timestamp

df_new = df_new.withColumn("update_date",current_timestamp())

df_new = df_new.withColumn("create_date",current_timestamp())


# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Surrogate key - From 1
# MAGIC
# MAGIC

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))

# COMMAND ----------

df.limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####**Adding Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsur = spark.sql("select max(DimCustomerKey)as max_surrogate_key from sk_databricks_cata.gold.DimCustomers")

# Converting df_maxsur to max_surrogate_key variable

    max_surrogate_key = int(df_maxsur.collect()[0]['max_surrogate_key'])

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

df_new.selectExpr("typeof(DimCustomerKey)").show()

# COMMAND ----------

from pyspark.sql.functions import col

df_new = df_new.filter(col("DimCustomerKey").cast("string") != "DimCustomerKey")

# COMMAND ----------

from pyspark.sql.functions import col

df_new = df_new.filter(col("DimCustomerKey").rlike("^[0-9]+$"))

# COMMAND ----------

df_new.select("DimCustomerKey").show(20, False)

# COMMAND ----------

from pyspark.sql.functions import expr

df_new = df_new.withColumn("DimCustomerKey",
    expr("try_cast(DimCustomerKey as BIGINT)")
)

# COMMAND ----------

df_new = df_new.filter("DimCustomerKey IS NOT NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Union of df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apply Upsert condition on Surrogate_Key

# COMMAND ----------

# MAGIC %md
# MAGIC ####SCD Type -1

# COMMAND ----------

if spark.catalog.tableExists("sk_databricks_cata.gold.DimCustomers"):
    print("Hello")
else:
    print("Bro")

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark.catalog.tableExists("sk_databricks_cata.gold.DimCustomers")):
     dlt_obj = DeltaTable.forPath(spark,"abfss://gold@skdatabricksete.dfs.core.windows.net/DimCustomers")


     dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey = src.DimCustomerKey")\
       .whenMatchedUpdateAll()\
       .whenNotMatchedInsertAll()\
       .execute()
  

else:
     df_final.write.mode("overwrite")\
        .format("delta")\
        .option("path","abfss://gold@skdatabricksete.dfs.core.windows.net/DimCustomers")\
        .saveAsTable("sk_databricks_cata.gold.DimCustomers")

      

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sk_databricks_cata.gold.dimcustomers