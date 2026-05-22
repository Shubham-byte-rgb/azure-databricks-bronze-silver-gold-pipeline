# azure-databricks-bronze-silver-gold-pipeline
End to End Data Engineering Pipeline using Azure Databricks, ADLS Gen2 and Delta Lake

Project Documentation: End-to-End Azure Databricks Data Platform

1. Project Overview
   
This project demonstrates the construction of a real-time, scalable data platform on Azure Databricks, adhering to best practices for data engineering. The solution covers incremental data loading, data quality enforcement, advanced transformations using PySpark and Python OOP, dimensional modelling (Star Schema) with Slowly Changing Dimensions (SCD Type 1 and Type 2), and orchestration using Databricks Workflows. The primary goal is to master Azure Databricks technology for real-world scenarios and interview preparation.

3. Architectural Design: Medallion Architecture
   
The project employs a Medallion Architecture to logically separate data based on quality and transformation levels, ensuring data integrity and reusability:

• Bronze Layer (Raw Data): Ingests raw data as-is from source systems with minimal or no transformations. Focuses on persistent storage of original data for auditing and reprocessing.
• Silver Layer (Enriched/Cleaned Data): Transforms and cleans data from the Bronze layer, applying business rules, standardising formats, and enriching data. This layer serves as a reliable source for downstream analytical applications.
• Gold Layer (Curated/Modell
ed Data): Structures the refined data into a dimensional model (Star Schema) consisting of fact and dimension tables. This layer is optimized for analytical queries and reporting.

3. Infrastructure Setup and Prerequisites
   
The project requires the following infrastructure and initial configurations:
• Azure Account: A free Azure account is created, providing $200 USD credits for services.
• Azure Data Lake Storage (ADLS Gen2): Used as the primary storage solution for the entire Medallion Architecture. ADLS Gen2 is configured by enabling hierarchical namespaces on a Blob Storage account.

    ◦ Containers:
        ▪ source: Stores raw incoming Parquet files (e.g., orders, customers, products, regions).
        ▪ bronze: Stores raw data after initial ingestion.
        ▪ silver: Stores cleaned and transformed data.
        ▪ gold: Stores curated data in Star Schema.
        ▪ metastore: Dedicated container for Unity Catalog's managed table storage.
        
• Azure Databricks Workspace: The core processing environment for Spark workloads.
    ◦ Access Connector for Azure Databricks: A crucial component to allow the Databricks workspace to access the Azure Data Lake Storage, bridging between the Databricks and Azure resources.
• Unity Catalog: A modern data governance solution for Databricks.
    ◦ Metastore: Configured in Azure Databricks to manage metadata across workspaces and enable Unity Catalog. A dedicated storage location (metastore container) is provided for managed tables.
    ◦ Credentials: An "access connector ID" is wrapped as a credential within Unity Catalog to manage access to ADLS containers.
    ◦ External Locations: Created for each Medallion layer (bronze, silver, gold, source) to map logical paths to physical storage locations in ADLS, promoting data isolation and governance.
    ◦ Catalogs and Schemas: A databrickskata catalog is created, with bronze, silver, and gold schemas to organize tables and functions.
    
4. Bronze Layer: Data Ingestion
   
This stage focuses on ingesting raw data incrementally and robustly into the Bronze layer.
• Source Data: Data is sourced from GitHub (simulating external systems) and stored as Parquet files in the source ADLS container. Parquet is chosen over CSV for its columnar format, schema-on-read capabilities, and efficiency for big data processing.
• Incremental Loading (Autoloader):
    ◦ Spark Structured Streaming: Utilized for continuous, incremental data ingestion.
    ◦ Autoloader (cloudFiles format): A Databricks feature built on Spark Structured Streaming that automatically processes new data files as they arrive in ADLS Gen2.
    ◦ Idempotency (Exactly-Once Processing): Achieved through checkpoint_location, which stores processing metadata (e.g., RoxDB folder) to ensure files are processed only once, even if the stream restarts.
    ◦ Schema Evolution (schema_location): Autoloader automatically infers and evolves schema changes. The inferred schema is stored in schema_location (typically within checkpoint_location). New or malformed columns are directed to a _rescued_data column.
    ◦ Dynamic Notebooks: A single, parameterized notebook handles incremental loading for multiple tables (e.g., orders, customers, products), making the solution scalable for hundreds of tables.
• Static Data Ingestion (No-Code Feature): For static mapping files (e.g., regions) that do not require incremental loading, Databricks' no-code "Data Ingestion" feature is used to quickly create managed Delta tables in the Bronze layer.
• Output Format: Data in the Bronze layer is stored in Parquet format.
5. Silver Layer: Data Transformation and Enrichment
The Silver layer cleans, refines, and enriches data from the Bronze layer, preparing it for analytical consumption.
• Technology: PySpark functions and Python OOP concepts (classes) are extensively used for transformations.
• Common Transformations:
    ◦ Dropping _rescued_data: The _rescued_data column, a result of schema evolution in the Bronze layer, is dropped if not needed for enrichment.
    ◦ Date/Time Conversions: Functions like to_timestamp and year are used to convert and extract components from date columns.
    ◦ String Manipulations: split for extracting domain names from emails, concat for creating full_name from first_name and last_name.
    ◦ Aggregation: group by and count to analyze data (e.g., top customer domains).
    ◦ Window Functions: dense_rank, rank, row_number for ranking and numbering rows based on partitions (e.g., ranking products by total amount within a year).
• Code Reusability (Python OOP): Python classes are created to encapsulate common transformation logic (e.g., Window class for window functions), promoting code reusability across notebooks.
• Unity Catalog Functions: User-defined functions (UDFs) are registered within Unity Catalog using SQL or Python. These functions persist across sessions and notebooks, enhancing reusability and governance (e.g., discount_func for price calculation, upper_func for string manipulation).
• Output Format: Data in the Silver layer is stored in Delta format, offering ACID properties, schema enforcement, and time travel capabilities.
6. Gold Layer: Data Modeling (Star Schema)
The Gold layer is where the Star Schema is built, comprising dimension and fact tables, optimized for analytical queries.
6.1 Slowly Changing Dimension Type 1 (SCD Type 1) - Customers Dimension
• Concept: SCD Type 1 handles changes by overwriting the existing record. No history is maintained; only the most current information is stored.
• Implementation: Manually coded using PySpark operations.
• Key Steps:
    ◦ Initial Load vs. Incremental Load: The notebook is designed to handle both the initial creation of the dimension table and subsequent incremental updates. A load_flag parameter (or spark.catalog.tableExists check) distinguishes between these scenarios.
    ◦ Duplicate Removal: Ensures uniqueness of records based on the natural key (e.g., customer_id).
    ◦ Surrogate Key Generation: A dim_customer_key is generated using monotonically_increasing_id() to serve as the primary key of the dimension table, starting from the maximum existing key for incremental loads.
    ◦ Record Identification: Joins the incoming data with the existing dimension table (or a pseudo-table for initial load) to identify new vs. old records.
    ◦ Metadata Columns: create_date (set on initial creation, never changed) and update_date (updated with current timestamp on every processing of the record) are added to track record lifecycle.
    ◦ Upsert Logic (MERGE): For incremental loads, the Delta MERGE statement is used to perform UPDATE for matching records (SCD Type 1 behavior) and INSERT for new records.
    ◦ Output Format: Data is stored as a Delta table.
6.2 Slowly Changing Dimension Type 2 (SCD Type 2) - Products Dimension
• Concept: SCD Type 2 preserves the full history of changes by creating a new record for each change, along with start_at and end_at columns to denote the active period of a record.
• Implementation: Utilizes Delta Live Tables (DLT) for automated SCD Type 2.
• Key DLT Features:
    ◦ Declarative ETL: DLT allows defining what needs to be achieved (e.g., an SCD Type 2 dimension) rather than how to implement it, abstracting complex logic.
    ◦ Streaming Tables & Views: DLT leverages streaming tables (unbounded tables for append-only data) and views for incremental processing.
    ◦ @dlt.table and @dlt.view Decorators: Python decorators are used to define DLT tables and views. dlt.read_stream("LIVE.table_name") is used to refer to other DLT assets within the pipeline.
    ◦ dlt.apply_changes API: This powerful API simplifies SCD implementation. It takes target, source, keys, sequence_by (for ordering changes), and stored_as_scd_type (set to 2 for SCD Type 2) as parameters, automating the complex logic of history tracking.
    ◦ Expectations: Data quality constraints (e.g., product_id IS NOT NULL, product_name IS NOT NULL) are applied using @dlt.expect_all_or_drop decorators. DLT can be configured to warn, drop records, or fail the pipeline upon expectation failure.
• Cluster Management: DLT pipelines require job clusters for execution and debugging. Specific attention is paid to ensuring sufficient core quotas and terminating all-purpose clusters before running DLT jobs to avoid resource conflicts.
6.3 Fact Table - Orders
• Purpose: The fact_orders table stores transactional data, linking to dimension tables using their surrogate keys.
• Implementation: PySpark operations, primarily joins.
• Key Steps:
    ◦ Data Reading: Reads the order_silver data from the Silver layer.
    ◦ Dimension Key Integration: Joins with dim_customers and dim_products (from the Gold layer) to retrieve dim_customer_key and dim_product_key, replacing the original natural keys.
    ◦ Column Selection: Only relevant measures and dimension keys are retained (e.g., order_id, dim_customer_key, dim_product_key, order_date, quantity, total_amount). Original natural keys (customer ID, product ID) are dropped after joining.
    ◦ Upsert Logic: Similar to SCD Type 1, the Delta MERGE statement is used for the fact table to handle updates and inserts based on a combination of dimension keys or a natural primary key (order_id) if available.
• Output Format: Data is stored as a Delta table.
7. Orchestration: Databricks Workflows (Jobs)
Databricks Workflows are used to orchestrate the entire end-to-end ETL pipeline, defining task dependencies and execution order.
• Parent Pipeline: A master workflow (End-to-End Pipeline) is created to manage all stages.
• Task Dependencies:
    ◦ Parameters Notebook: Runs first to define dynamic parameters for the Bronze layer.
    ◦ Bronze Autoloader: Runs next, utilizing the parameters to incrementally load data for all source tables.
    ◦ Silver Layer Notebooks: silver_orders, silver_customers, silver_products are executed in parallel, as they are independent of each other and depend only on the completion of the bronze_autoloader task.
    ◦ Gold Layer Dimensions: gold_customers (SCD Type 1) and gold_products (DLT for SCD Type 2) tasks are executed in parallel, dependent on the completion of all Silver layer notebooks.
    ◦ Gold Layer Fact Table: The fact_orders task runs last, dependent on the successful completion of both Gold layer dimension tasks.
• Dynamic Execution: The workflow leverages parameterized notebooks and loops (for-each activity) to process multiple tables with a single notebook, enhancing scalability and maintainability.
8. Data Warehousing and BI Integration
Once the Gold layer is populated, Databricks provides tools for data warehousing and integration with BI tools.
• Databricks SQL Warehouse: Optimized compute specifically designed for running SQL workloads. It offers serverless SQL endpoints for efficient querying of the Gold layer.
• SQL Editor: Allows data engineers and analysts to write and execute SQL queries directly against the Delta tables in the Gold layer, save queries, and build basic visualizations.
• Partner Connect: Provides seamless integration with external BI tools like Power BI and Tableau. Users can download pre-configured connection files (e.g., .pbix for Power BI) that abstract away connection complexities, enabling quick dashboard creation.
