# Databricks notebook source
# Install faker if not installed
%pip install faker

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

import sys

# COMMAND ----------

# Ensure the module path is correctly added
module_path = '/Workspace/Users/labuser7046430@vocareum.com/advanced-data-engineering-with-databricks-3.4.1/Synthetic_Data'
if module_path not in sys.path:
    sys.path.append(module_path)

# Add debug prints to verify paths
print("Module Path:", module_path)
print("Sys Path:", sys.path)

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
import os
import sys
from data_generation import TestProductData, TestSalesData

# COMMAND ----------

product_test_data = TestProductData(spark, num_rows=100)
product_test_data_df = product_test_data.create_dataframe()



display(product_test_data_df)


# COMMAND ----------

# Generate customer data using TestSalesData
sales_test_data = TestSalesData(spark, product_test_data_df, num_rows=100)
sales_test_data_df = sales_test_data.create_dataframe()

display(sales_test_data_df)

# COMMAND ----------

# Define catalog and table names
catalog = "test_data.synthetic_data"
product_table = f"{catalog}.product_data"
sales_table = f"{catalog}.sales_data"

# COMMAND ----------

# Create catalog and tables if they don't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog.split('.')[0]}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}")

# COMMAND ----------

# Save product data to the catalog
product_df.write.mode("overwrite").saveAsTable(product_table)

# Save sales data to the catalog
sales_df.write.mode("overwrite").saveAsTable(sales_table)

# COMMAND ----------

# Display success message
print(f"Successfully saved {num_product_rows} rows of product data to {product_table}")
print(f"Successfully saved {num_sales_rows} rows of sales data to {sales_table}")
