# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name","") 

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------


df = spark.read.format("delta") \
  .load("abfss://bronze@skdatabricksete.dfs.core.windows.net/orders")

df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **To Check the Table** 

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://source@skdatabricksete.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**
# MAGIC  Autoloader (Source to Bronze)
# MAGIC  
# MAGIC  Purpose
# MAGIC 	•	Incrementally ingest Parquet files from the Source container
# MAGIC 	•	Store schema metadata and file tracking information in the Bronze container
# MAGIC
# MAGIC 🔹 Key Concepts
# MAGIC 	•	cloudFiles → Enables Databricks Auto Loader
# MAGIC 	•	cloudFiles.format = parquet → Specifies file format
# MAGIC 	•	cloudFiles.schemaLocation → Stores schema metadata and processed file state
# MAGIC 	•	.load(source_path) → Reads raw files from landing zone
# MAGIC
# MAGIC 🔹 Architecture Flow
# MAGIC 	•	Source container acts as landing/raw zone
# MAGIC 	•	Auto Loader detects new files automatically
# MAGIC 	•	Schema and ingestion metadata stored in Bronze
# MAGIC 	•	Data loaded into Spark DataFrame
# MAGIC
# MAGIC 🔹 Why This Is Used
# MAGIC 	•	Handles incremental file ingestion
# MAGIC 	•	Supports schema evolution
# MAGIC 	•	Scalable for production workloads
# MAGIC 	•	Avoids manual file tracking

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format","parquet")\
  .option("cloudFiles.schemaLocation",f"abfss://bronze@skdatabricksete.dfs.core.windows.net/checkpoint_{p_file_name}")\
  .load(f"abfss://source@skdatabricksete.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Write 
# MAGIC (Bronze Layer)
# MAGIC
# MAGIC Incremental Ingestion – Source to Bronze (Databricks)
# MAGIC
# MAGIC Objective
# MAGIC
# MAGIC To incrementally ingest Parquet files from the Source container into the Bronze layer using Databricks Auto Loader with fault-tolerant streaming.
# MAGIC
# MAGIC ⸻
# MAGIC
# MAGIC Approach
# MAGIC 	•	Used format("cloudFiles") (Auto Loader) for incremental file detection
# MAGIC 	•	Source container acts as landing zone
# MAGIC 	•	Streaming write with outputMode("append")
# MAGIC 	•	Separate checkpointLocation for state management
# MAGIC 	•	Used trigger(once=True) for batch-style incremental execution
# MAGIC
# MAGIC ⸻
# MAGIC
# MAGIC Key Design Principles
# MAGIC 	•	Append-only ingestion
# MAGIC 	•	Separate data and checkpoint paths
# MAGIC 	•	Exactly-once processing
# MAGIC 	•	Schema evolution support
# MAGIC 	•	Medallion architecture compliant
# MAGIC
# MAGIC ⸻
# MAGIC
# MAGIC Execution Flow
# MAGIC
# MAGIC Source → Auto Loader → Bronze Layer
# MAGIC
# MAGIC ⸻
# MAGIC
# MAGIC Production Note
# MAGIC
# MAGIC Delta format is recommended over Parquet for ACID transactions and better reliability.

# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation",f"abfss://bronze@skdatabricksete.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path",f"abfss://bronze@skdatabricksete.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC See the output 

# COMMAND ----------

spark.read.format("delta")\
.load("abfss://bronze@skdatabricksete.dfs.core.windows.net/orders")\
.display()

# COMMAND ----------

df = spark.read.format("delta")\
    .load(f"abfss://bronze@skdatabricksete.dfs.core.windows.net/{p_file_name}")
display(df)

# COMMAND ----------

print(p_file_name)