from faker.providers import BaseProvider
from faker import Faker
import random
import string

# Define a custom provider class
class SalesDataProvider(BaseProvider):

    def sales_transaction_date(self):
        fake = Faker()
        return fake.date_between(start_date="-1y", end_date="today")

    def sales_transaction_time(self):
        fake = Faker()
        return fake.time()

    def sales_quantity(self):
        return random.randint(1, 20)

    def sales_price(self, price):
        return round(random.uniform(0.8, 1.2) * price, 2)

    def customer_id(self):
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

    def transaction_id(self):
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

    def store_id(self):
        return random.choice(['Store-001', 'Store-002', 'Store-003', 'Store-004'])

    def payment_method(self):
        return random.choice(['Credit Card', 'Cash', 'Debit Card', 'Mobile Payment'])
