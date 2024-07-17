# Databricks notebook source
# Install faker if not installed
# %pip install faker

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
import os
import sys
from data_generation import TestProductData, TestSalesData

# COMMAND ----------

# Initialize Faker and add the custom providers
fake = Faker('de_DE')
fake.add_provider(ProductDataProvider)
fake.add_provider(SalesDataProvider)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("Synthetic Data Generation").getOrCreate()

# Define the number of rows for product and sales data
num_product_rows = 100
num_sales_rows = 1000

# COMMAND ----------

# Generate synthetic product data
product_data_generator = TestProductData(spark, num_rows=num_product_rows)
product_df = product_data_generator.create_dataframe()

# COMMAND ----------

# Display a sample of the data
display(product_df)

# COMMAND ----------

# Generate synthetic sales data
sales_data_generator = TestSalesData(spark, product_df, num_rows=num_sales_rows)
sales_df = sales_data_generator.create_dataframe()

# COMMAND ----------

# Display a sample of the data
display(sales_df)

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
