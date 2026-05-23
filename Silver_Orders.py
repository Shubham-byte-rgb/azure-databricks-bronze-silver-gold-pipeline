# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC Import to_timestamp AND Important function 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank, desc

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://bronze@skdatabricksete.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename the Rescued_data column with pyspark

# COMMAND ----------

df=df.withColumnRenamed("_rescued_data","rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the rescued_column

# COMMAND ----------

df = df.drop("rescued_data")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Date transformation in Pyspark
# MAGIC convert date type into timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df = df.withColumn("order_date", to_timestamp("order_date"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###create a New Column
# MAGIC Year,month
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import year
from pyspark.sql.functions import month
df = df.withColumn("year",year('order_date'))
df.display()
df = df.withColumn("month",month('order_date'))
df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ###Windows Functions
# MAGIC Dense_rank

# COMMAND ----------

df1 = df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rank

# COMMAND ----------

df1 = df1.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Row number

# COMMAND ----------

df1 = df1.withColumn("row_flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Classes - OOP

# COMMAND ----------

class windows :
    
    def dense_rank(self,df):
        
        df_dense_rank = df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

        return df_dense_rank
    
    def rank(self,df):
        
        df_rank = df.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

        return df_rank
    
    def row_number(self,df):
        
        df_row_number = df.withColumn("row_flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

        return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create a object to call the window functions

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing
# MAGIC to write a data in silver layer

# COMMAND ----------

from pyspark.sql.functions import to_date, col

df = df.withColumn("order_date", to_date(col("order_date")))

# COMMAND ----------

df.write.format("delta")\
.mode("overwrite")\
.option("overwriteSchema","true")\
.save("abfss://silver@skdatabricksete.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Table on top of it

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists sk_databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@skdatabricksete.dfs.core.windows.net/orders"