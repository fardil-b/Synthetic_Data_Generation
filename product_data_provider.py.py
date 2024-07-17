# Databricks notebook source
from faker.providers import BaseProvider
from faker import Faker
import random
import string
from datetime import datetime, timedelta

class GermanProductDataProvider(BaseProvider):

    def product_id(self):
        letters = ''.join(random.choices(string.ascii_uppercase, k=2))
        numbers = ''.join(random.choices(string.digits, k=4))
        return f'PROD-{letters}-{numbers}'

    def product_category(self):
        categories = list(self.product_data.keys())
        return self.random_element(categories)

    def product_subcategory(self, category):
        subcategories = self.product_data.get(category, {}).keys()
        return self.random_element(list(subcategories))

    def product_name(self, category, subcategory):
        return self.random_element(self.product_data.get(category, {}).get(subcategory, ["Unknown Product"]))

    def generate_brand_name(self, min_length=10, max_length=30):
        fake = Faker()
        while True:
            brand_name = fake.company()
            if min_length <= len(brand_name) <= max_length:
                return brand_name

    def product_price(self, category, subcategory):
        price_ranges = self.price_data.get(category, {}).get(subcategory, (1.00, 10.00))
        return round(random.uniform(*price_ranges), 2)

    def stock_quantity(self):
        return random.randint(1, 500)

    def manufacture_date(self):
        fake = Faker()
        return fake.date_this_year()

    def expiry_date(self, manufacture_date):
        expiry_date_obj = manufacture_date + timedelta(days=random.randint(1, 365))
        return expiry_date_obj

    product_data = {
        "Beverages": {
            "Soft Drinks": ["Coca Cola", "Pepsi", "Fanta", "Sprite"],
            "Juices": ["Hohes C Orange Juice", "Granini Apple Juice", "Rauch Multivitamin Juice"],
            "Water": ["Gerolsteiner Mineral Water", "Volvic Mineral Water"],
            "Coffee": ["Dallmayr Prodomo Coffee", "Tchibo Gold Selection"],
            "Tea": ["Teekanne Green Tea", "Messmer Black Tea"],
            "Alcoholic Beverages": ["Jägermeister", "Beck's Beer", "Franziskaner Weissbier"],
            "Wine": ["Riesling", "Spätburgunder", "Grauburgunder"]
        },
        "Bakery": {
            "Bread": ["Vollkornbrot", "Roggenbrot", "Brezel"],
            "Pastries": ["Butter Croissant", "Apfeltasche"],
            "Cakes": ["Schwarzwälder Kirschtorte", "Sachertorte"],
            "Cookies": ["Leibniz Butterkeks", "Bahlsen Choco Leibniz"],
            "Bagels": ["Sesam Bagels", "Mehrkorn Bagels"]
        },
        "Dairy": {
            "Milk": ["Vollmilch", "Magermilch", "Sojamilch"],
            "Cheese": ["Gouda", "Edamer", "Emmentaler"],
            "Yogurt": ["Müller Joghurt", "Alpro Sojajoghurt"],
            "Butter": ["Kerrygold Butter", "Deutsche Markenbutter"],
            "Cream": ["Schlagsahne", "Crème Fraîche"]
        },
        "Meat & Seafood": {
            "Beef": ["Rinderhackfleisch", "Rumpsteak", "Rinderrouladen"],
            "Chicken": ["Hähnchenbrustfilet", "Hähnchenschenkel", "Ganzes Huhn"],
            "Pork": ["Schweinekotelett", "Bauchspeck", "Schweinefilet"],
            "Fish": ["Lachs", "Forelle", "Kabeljau"],
            "Shellfish": ["Garnelen", "Krabben", "Hummer"]
        },
        "Produce": {
            "Fruits": ["Äpfel", "Bananen", "Orangen"],
            "Vegetables": ["Karotten", "Brokkoli", "Spinat"],
            "Fresh Herbs": ["Basilikum", "Koriander", "Petersilie"],
            "Salad Mixes": ["Ceasar Salat", "Gemischter Salat"]
        },
        "Frozen Foods": {
            "Frozen Meals": ["Tiefkühl-Lasagne", "Tiefkühl-Pizza"],
            "Ice Cream": ["Vanilleeis", "Schokoladeneis"],
            "Frozen Vegetables": ["Tiefkühlerbsen", "Tiefkühlmais"],
            "Frozen Pizza": ["Salami Pizza", "Margherita Pizza"],
            "Frozen Seafood": ["Tiefkühlgarnelen", "Tiefkühlfischfilets"]
        },
        "Pantry Staples": {
            "Pasta": ["Spaghetti", "Penne", "Maccheroni"],
            "Rice": ["Weißer Reis", "Brauner Reis"],
            "Canned Goods": ["Gekochte Bohnen", "Gehackte Tomaten"],
            "Baking Supplies": ["Weizenmehl", "Backpulver"],
            "Spices & Seasonings": ["Salz", "Schwarzer Pfeffer"]
        },
        "Snacks": {
            "Chips": ["Kartoffelchips", "Tortilla Chips"],
            "Crackers": ["Salzcracker", "Käsecracker"],
            "Nuts": ["Mandeln", "Erdnüsse"],
            "Popcorn": ["Butter Popcorn", "Karamell Popcorn"],
            "Candy": ["Schokoriegel", "Gummibärchen"]
        },
        "Health & Beauty": {
            "Personal Care Products": ["Shampoo", "Duschgel"],
            "Skincare": ["Feuchtigkeitscreme", "Gesichtsreiniger"],
            "Haircare": ["Haargel", "Haarspray"],
            "Vitamins & Supplements": ["Vitamin C", "Multivitamine"]
        },
        "Household Supplies": {
            "Cleaning Products": ["Desinfektionsspray", "Glasreiniger"],
            "Paper Goods": ["Papierhandtücher", "Toilettenpapier"],
            "Laundry Supplies": ["Waschmittel", "Weichspüler"],
            "Dishwashing Supplies": ["Spülmittel", "Geschirrspülmittel"]
        },
        "Baby Products": {
            "Baby Food": ["Babybrei", "Fruchtpüree"],
            "Diapers": ["Windeln Größe 1", "Windeln Größe 2"],
            "Baby Care Products": ["Baby Lotion", "Baby Shampoo"],
            "Baby Formula": ["Babynahrung", "Säuglingsnahrung"]
        },
        "Pet Supplies": {
            "Pet Food": ["Hundefutter", "Katzenfutter"],
            "Pet Toys": ["Kauspielzeug", "Katzenminze Spielzeug"],
            "Pet Health Products": ["Flohbehandlung", "Haustier Vitamine"],
            "Pet Accessories": ["Halsband", "Leine"]
        },
        "Deli": {
            "Sliced Meats": ["Schinken", "Putenbrust"],
            "Cheeses": ["Schweizer Käse", "Provolone Käse"],
            "Prepared Salads": ["Nudelsalat", "Kartoffelsalat"],
            "Sandwiches": ["Puten-Sandwich", "Schinken-Sandwich"]
        },
        "Prepared Foods": {
            "Ready-to-Eat Meals": ["Makkaroni & Käse", "Hähnchen Alfredo"],
            "Rotisserie Chicken": ["Ganzes Hähnchen vom Grill", "Halbes Hähnchen vom Grill"],
            "Salads": ["Griechischer Salat", "Cobb Salat"],
            "Soups": ["Hühnersuppe", "Tomatensuppe"]
        },
        "International Foods": {
            "Asian Cuisine": ["Sushi", "Frühlingsrollen"],
            "Latin American Cuisine": ["Tacos", "Burritos"],
            "Mediterranean Cuisine": ["Hummus", "Falafel"],
            "Indian Cuisine": ["Chicken Tikka Masala", "Naan Brot"]
        },
        "Organic & Specialty Foods": {
            "Gluten-Free Products": ["Glutenfreies Brot", "Glutenfreie Pasta"],
            "Vegan Products": ["Veganer Käse", "Vegane Wurst"],
            "Organic Produce": ["Bio Äpfel", "Bio Spinat"],
            "Specialty Diet Products": ["Keto Riegel", "Natriumarme Suppe"]
        }
    }

    price_data = {
        "Beverages": {
            "Soft Drinks": (0.70, 2.50),
            "Juices": (1.50, 3.50),
            "Water": (0.30, 1.20),
            "Coffee": (3.00, 7.00),
            "Tea": (2.00, 5.00),
            "Alcoholic Beverages": (5.00, 30.00),
            "Wine": (4.00, 25.00)
        },
        "Bakery": {
            "Bread": (1.00, 4.00),
            "Pastries": (1.00, 3.50),
            "Cakes": (5.00, 20.00),
            "Cookies": (1.50, 4.00),
            "Bagels": (1.00, 3.00)
        },
        "Dairy": {
            "Milk": (0.70, 1.50),
            "Cheese": (2.00, 10.00),
            "Yogurt": (1.00, 3.00),
            "Butter": (1.50, 3.00),
            "Cream": (1.50, 4.00)
        },
        "Meat & Seafood": {
            "Beef": (6.00, 25.00),
            "Chicken": (3.00, 10.00),
            "Pork": (4.00, 15.00),
            "Fish": (5.00, 20.00),
            "Shellfish": (8.00, 30.00)
        },
        "Produce": {
            "Fruits": (1.00, 6.00),
            "Vegetables": (0.50, 4.00),
            "Fresh Herbs": (1.00, 3.00),
            "Salad Mixes": (2.00, 5.00)
        },
        "Frozen Foods": {
            "Frozen Meals": (3.00, 7.00),
            "Ice Cream": (2.00, 5.00),
            "Frozen Vegetables": (1.00, 3.00),
            "Frozen Pizza": (3.00, 8.00),
            "Frozen Seafood": (4.00, 12.00)
        },
        "Pantry Staples": {
            "Pasta": (1.00, 3.00),
            "Rice": (1.00, 4.00),
            "Canned Goods": (0.70, 2.50),
            "Baking Supplies": (1.00, 4.00),
            "Spices & Seasonings": (1.00, 3.00)
        },
        "Snacks": {
            "Chips": (1.00, 3.00),
            "Crackers": (1.00, 3.00),
            "Nuts": (2.00, 6.00),
            "Popcorn": (1.00, 4.00),
            "Candy": (1.00, 3.00)
        },
        "Health & Beauty": {
            "Personal Care Products": (2.00, 8.00),
            "Skincare": (5.00, 25.00),
            "Haircare": (3.00, 12.00),
            "Vitamins & Supplements": (3.00, 20.00)
        },
        "Household Supplies": {
            "Cleaning Products": (2.00, 10.00),
            "Paper Goods": (1.00, 6.00),
            "Laundry Supplies": (2.00, 8.00),
            "Dishwashing Supplies": (1.50, 5.00)
        },
        "Baby Products": {
            "Baby Food": (1.00, 4.00),
            "Diapers": (5.00, 15.00),
            "Baby Care Products": (2.00, 8.00),
            "Baby Formula": (10.00, 25.00)
        },
        "Pet Supplies": {
            "Pet Food": (2.00, 10.00),
            "Pet Toys": (1.00, 6.00),
            "Pet Health Products": (3.00, 10.00),
            "Pet Accessories": (2.00, 8.00)
        },
        "Deli": {
            "Sliced Meats": (3.00, 8.00),
            "Cheeses": (4.00, 10.00),
            "Prepared Salads": (3.00, 6.00),
            "Sandwiches": (2.50, 5.00)
        },
        "Prepared Foods": {
            "Ready-to-Eat Meals": (4.00, 10.00),
            "Rotisserie Chicken": (5.00, 12.00),
            "Salads": (3.00, 7.00),
            "Soups": (2.50, 5.00)
        },
        "International Foods": {
            "Asian Cuisine": (5.00, 15.00),
            "Latin American Cuisine": (4.00, 12.00),
            "Mediterranean Cuisine": (6.00, 18.00),
            "Indian Cuisine": (5.00, 15.00)
        },
        "Organic & Specialty Foods": {
            "Gluten-Free Products": (3.00, 10.00),
            "Vegan Products": (4.00, 12.00),
            "Organic Produce": (1.00, 6.00),
            "Specialty Diet Products": (4.00, 15.00)
        }
    }

# Register the provider with Faker
fake = Faker()
fake.add_provider(GermanProductDataProvider)

# Example usage
print(fake.product_id())
print(fake.product_category())
category = fake.product_category()
subcategory = fake.product_subcategory(category)
print(fake.product_name(category, subcategory))
print(fake.generate_brand_name())
print(fake.product_price(category, subcategory))
print(fake.stock_quantity())
manufacture_date = fake.manufacture_date()
print(manufacture_date)
print(fake.expiry_date(manufacture_date))

