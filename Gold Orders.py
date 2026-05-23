# Databricks notebook source
# MAGIC %md FACT ORDERS
# MAGIC -
# MAGIC Data Reading
# MAGIC

# COMMAND ----------

df = spark.sql("select * from sk_databricks_cata.silver.orders_silver")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sk_databricks_cata.gold.dimproducts

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from sk_databricks_cata.gold.dimcustomers")

df_dimpro = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from sk_databricks_cata.gold.dimproducts")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC FACT DATAFRAME

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_customer_id'],how='left').join(df_dimpro,df['product_id'] == df_dimpro['dim_product_id'],how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert on Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

from delta.tables import DeltaTable

try:
    print("Trying MERGE...")

    dlt_obj = DeltaTable.forName(spark, "sk_databricks_cata.gold.FactOrders")

    dlt_obj.alias("trg").merge(
        df_fact_new.alias("src"),
        "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("MERGE successful ✅")

except Exception as e:
    print("Table not ready / not delta, creating fresh table...")

    df_fact_new.write.format("delta") \
        .mode("overwrite") \
        .option("path", "abfss://gold@skdatabricksete.dfs.core.windows.net/FactOrders") \
        .saveAsTable("sk_databricks_cata.gold.FactOrders")

    print("Table created ✅")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sk_databricks_cata.gold.factorders;