import requests
import pymongo
import time
from pymongo import MongoClient
from datetime import datetime

# Configuración de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['data_bsale']

# Configuración de las APIs
apis = {
    "shipping": "https://api.bsale.cl/v1/shippings.json?expand=[user, office, shipping_type, guide, details]",
    "returns": "https://api.bsale.cl/v1/shippings.json?expand=[user, office, shipping_type, guide, details]",
    "products": "https://api.bsale.cl/v1/products.json?expand=[product_type, variants, product_taxes]",
    "variants": "https://api.bsale.cl/v1/variants.json?expand=[product, costs]",
    "receptions": "https://api.bsale.cl/v1/stocks/receptions.json?expand=[office, user, details]",
    "price_lists": "https://api.bsale.cl/v1/price_lists.json?expand=details",
    "clients": "https://api.bsale.cl/v1/clients.json?expand=[payment_type, sale_condition, price_list, contacts, addresses]",
    "payments": "https://api.bsale.cl/v1/payments.json?expand=[payment_type, document, office, user]",
    "group_payment_types": "https://api.bsale.cl/v1/payments/group_payment_types.json?expand=details",
    "offices": "https://api.bsale.cl/v1/offices.json",
    "users": "https://api.bsale.cl/v1/users.json?expand=office",
    "document_types": "https://api.bsale.cl/v1/document_types.json?expand=book_type",
    "shipping_types": "https://api.bsale.cl/v1/shipping_types.json",
    "payment_types": "https://api.bsale.cl/v1/payment_types.json?expand=dynamic_attributes",
    "sale_conditions": "https://api.bsale.cl/v1/sale_conditions.json",
    "book_types": "https://api.bsale.cl/v1/book_types.json",
    "dte_codes": "https://api.bsale.cl/v1/dte_codes.json",
    "taxes": "https://api.bsale.cl/v1/taxes.json",
    "documents": "https://api.bsale.cl/v1/documents.json?emissiondaterange=[1717200000,1719792000]"
}

headers = {
    "access_token": "e57d148002d91e58f152bece58d810e50bc84286"
}

def fetch_and_store(api_name, url, headers, retries=3):
    collection = db[api_name]
    next_url = url

    while next_url:
        for attempt in range(retries):
            try:
                print(f"Fetching data from {api_name} (attempt {attempt + 1})")
                response = requests.get(next_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                print(f"Data fetched from {api_name}")

                # Verificar si la respuesta es paginada
                if isinstance(data, dict) and 'items' in data:
                    items = data['items']
                    next_url = data.get('next')  # URL de la siguiente página
                else:
                    items = data
                    next_url = None

                # Filtrar los datos por fecha
                filtered_items = []
                for item in items:
                    # Asumiendo que el campo de fecha es 'created_at'
                    created_at = item.get('created_at')
                    if created_at:
                        created_at_date = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S')
                        if created_at_date >= datetime(2024, 6, 1):
                            filtered_items.append(item)

                print(f"Filtered {len(filtered_items)} items from {api_name}")

                if filtered_items:
                    collection.insert_many(filtered_items)
                    print(f"Data from {api_name} stored successfully in MongoDB.")

                break
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from {api_name}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                print(f"Error storing data for {api_name}: {e}")
                break

def fetch_shipping():
    print("Starting fetch for shipping data")
    fetch_and_store("shipping", apis["shipping"], headers)
    print("Completed fetch for shipping data")

def fetch_returns():
    print("Starting fetch for returns data")
    fetch_and_store("returns", apis["returns"], headers)
    print("Completed fetch for returns data")

def fetch_products():
    print("Starting fetch for products data")
    fetch_and_store("products", apis["products"], headers)
    print("Completed fetch for products data")

def fetch_variants():
    print("Starting fetch for variants data")
    fetch_and_store("variants", apis["variants"], headers)
    print("Completed fetch for variants data")

def fetch_receptions():
    print("Starting fetch for receptions data")
    fetch_and_store("receptions", apis["receptions"], headers)
    print("Completed fetch for receptions data")

def fetch_price_lists():
    print("Starting fetch for price lists data")
    fetch_and_store("price_lists", apis["price_lists"], headers)
    print("Completed fetch for price lists data")

def fetch_clients():
    print("Starting fetch for clients data")
    fetch_and_store("clients", apis["clients"], headers)
    print("Completed fetch for clients data")

def fetch_payments():
    print("Starting fetch for payments data")
    fetch_and_store("payments", apis["payments"], headers)
    print("Completed fetch for payments data")

def fetch_group_payment_types():
    print("Starting fetch for group payment types data")
    fetch_and_store("group_payment_types", apis["group_payment_types"], headers)
    print("Completed fetch for group payment types data")

def fetch_offices():
    print("Starting fetch for offices data")
    fetch_and_store("offices", apis["offices"], headers)
    print("Completed fetch for offices data")

def fetch_users():
    print("Starting fetch for users data")
    fetch_and_store("users", apis["users"], headers)
    print("Completed fetch for users data")

def fetch_document_types():
    print("Starting fetch for document types data")
    fetch_and_store("document_types", apis["document_types"], headers)
    print("Completed fetch for document types data")

def fetch_shipping_types():
    print("Starting fetch for shipping types data")
    fetch_and_store("shipping_types", apis["shipping_types"], headers)
    print("Completed fetch for shipping types data")

def fetch_payment_types():
    print("Starting fetch for payment types data")
    fetch_and_store("payment_types", apis["payment_types"], headers)
    print("Completed fetch for payment types data")

def fetch_sale_conditions():
    print("Starting fetch for sale conditions data")
    fetch_and_store("sale_conditions", apis["sale_conditions"], headers)
    print("Completed fetch for sale conditions data")

def fetch_book_types():
    print("Starting fetch for book types data")
    fetch_and_store("book_types", apis["book_types"], headers)
    print("Completed fetch for book types data")

def fetch_dte_codes():
    print("Starting fetch for dte codes data")
    fetch_and_store("dte_codes", apis["dte_codes"], headers)
    print("Completed fetch for dte codes data")

def fetch_taxes():
    print("Starting fetch for taxes data")
    fetch_and_store("taxes", apis["taxes"], headers)
    print("Completed fetch for taxes data")

def fetch_documents():
    print("Starting fetch for documents data")
    fetch_and_store("documents", apis["documents"], headers)
    print("Completed fetch for documents data")

# Llamadas a las funciones para cada API
fetch_shipping()
fetch_returns()
fetch_products()
fetch_variants()
fetch_receptions()
fetch_price_lists()
fetch_clients()
fetch_payments()
fetch_group_payment_types()
fetch_offices()
fetch_users()
fetch_document_types()
fetch_shipping_types()
fetch_payment_types()
fetch_sale_conditions()
fetch_book_types()
fetch_dte_codes()
fetch_taxes()
fetch_documents()

print("Data fetching and storing process completed.")
