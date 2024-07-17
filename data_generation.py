"""
Module for generating synthetic product and sales test data using Faker and dbldatagen.
"""

import os
import sys
import random
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from dbldatagen import DataGenerator

sys.path.append(os.path.dirname(__file__))
from product_data_provider import ProductDataProvider
from sales_data_provider import SalesDataProvider


class TestProductData:
    """
    This class generates product test data using the Faker library and dbldatagen.
    """

    def __init__(self, spark: SparkSession, num_rows: int = 10):
        """
        Initializes the TestProductData object with the SparkSession
        and the number of rows for the test data.

        Args:
            spark (SparkSession): SparkSession object.
            num_rows (int): Number of rows for the test data. Default is 10.
        """
        self.spark = spark
        self.num_rows = num_rows
        self.fake = Faker()
        self.fake.add_provider(ProductDataProvider)

    def create_dataframe(self):
        """
        Creates a Spark DataFrame from the generated product test data using Faker and dbldatagen.

        Returns:
            DataFrame: A Spark DataFrame containing the test data.
        """
        # Generate Faker data using custom provider
        product_ids = [self.fake.product_id() for _ in range(self.num_rows)]
        categories = [self.fake.product_category() for _ in range(self.num_rows)]
        subcategories = [self.fake.product_subcategory(cat) for cat in categories]
        product_names = [
            self.fake.product_name(cat, subcat) for cat, subcat in zip(categories, subcategories)
        ]
        brand_names = [self.fake.generate_brand_name() for _ in range(self.num_rows)]
        prices = [
            self.fake.product_price(cat, subcat) for cat, subcat in zip(categories, subcategories)
        ]
        stock_quantities = [self.fake.stock_quantity() for _ in range(self.num_rows)]
        manufacture_dates = [self.fake.manufacture_date() for _ in range(self.num_rows)]
        expiry_dates = [self.fake.expiry_date(mdate) for mdate in manufacture_dates]

        # Use dbldatagen to generate data
        data_gen = (
            DataGenerator(spark=self.spark, name="product_data", rows=self.num_rows, partitions=1)
            .withIdOutput()
            .withColumn("product_id", StringType(), values=product_ids)
            .withColumn("category", StringType(), values=categories)
            .withColumn("subcategory", StringType(), values=subcategories)
            .withColumn("product_name", StringType(), values=product_names)
            .withColumn("brand_name", StringType(), values=brand_names)
            .withColumn("price", FloatType(), values=prices)
            .withColumn("stock_quantity", IntegerType(), values=stock_quantities)
            .withColumn("manufacture_date", DateType(), values=manufacture_dates)
            .withColumn("expiry_date", DateType(), values=expiry_dates)
        )

        return data_gen.build()


class TestSalesData:
    """
    This class generates historical sales data based on product data
    using the Faker library and dbldatagen.
    """

    def __init__(self, spark: SparkSession, product_test_data_df, num_rows: int = 1000):
        """
        Initializes the Input2 object with the SparkSession, product DataFrame,
        and the number of sales transactions.

        Args:
            spark (SparkSession): SparkSession object.
            product_test_data_df (DataFrame): Spark DataFrame containing product data.
            num_rows (int): Number of rows for the sales data. Default is 1000.
        """
        self.spark = spark
        self.product_test_data_df = product_test_data_df
        self.num_rows = num_rows
        self.fake = Faker()
        self.fake.add_provider(SalesDataProvider)

    def create_dataframe(self):
        """
        Creates a Spark DataFrame from the generated sales data using Faker and dbldatagen.

        Returns:
            DataFrame: A Spark DataFrame containing the sales data.
        """
        product_data = self.product_test_data_df.collect()

        transaction_ids = [self.fake.transaction_id() for _ in range(self.num_rows)]
        customer_ids = [self.fake.customer_id() for _ in range(self.num_rows)]
        store_ids = [self.fake.store_id() for _ in range(self.num_rows)]
        product_ids = [random.choice(product_data).product_id for _ in range(self.num_rows)]
        transaction_dates = [self.fake.sales_transaction_date() for _ in range(self.num_rows)]
        transaction_times = [self.fake.sales_transaction_time() for _ in range(self.num_rows)]
        quantities = [self.fake.sales_quantity() for _ in range(self.num_rows)]
        payment_methods = [self.fake.payment_method() for _ in range(self.num_rows)]
        prices = []

        for i in range(self.num_rows):
            product = next((p for p in product_data if p.product_id == product_ids[i]), None)
            if product:
                sale_price = self.fake.sales_price(product.price)
                prices.append(sale_price)

        data_gen = (
            DataGenerator(spark=self.spark, name="sales_data", rows=self.num_rows, partitions=1)
            .withIdOutput()
            .withColumn("transaction_id", StringType(), values=transaction_ids)
            .withColumn("customer_id", StringType(), values=customer_ids)
            .withColumn("store_id", StringType(), values=store_ids)
            .withColumn("product_id", StringType(), values=product_ids)
            .withColumn("transaction_date", DateType(), values=transaction_dates)
            .withColumn("transaction_time", StringType(), values=transaction_times)
            .withColumn("quantity", IntegerType(), values=quantities)
            .withColumn("payment_method", StringType(), values=payment_methods)
            .withColumn("sales_price", FloatType(), values=prices)
        )

        return data_gen.build()
