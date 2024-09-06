import requests
from pymongo import MongoClient, errors


def fetch_product_type(apiurl, access_token):
    headers = {
        'access_token': access_token
    }
    response = requests.get(apiurl, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener product type {apiurl}: {response.status_code}")
        return None
    




def save_typeProducts_to_mongodb(product_data):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    client_collection = db['type_products']
    
    # Crear índice único en el campo 'id' para evitar duplicados
    client_collection.create_index("id", unique=True)

    for type_product in product_data:
        try:
            client_collection.update_one(
                {"id": product["id"]}, 
                {"$set": product}, 
                upsert=True
            )
            print(f"Tipo de Producto con ID {type_product['id']} insertado/actualizado en la Base de Datos.")
        except errors.DuplicateKeyError:
            print(f"Producto con ID {type_product['id']} ya existe en la Base de Datos.")
        except Exception as e:
            print(f"Error al insertar/actualizar el producto con ID {product['id']}: {str(e)}")



if __name__ == "__main__":
    api_urls = [
        "https://api.bsale.cl/v1/product_types/3/products.json",
        "https://api.bsale.cl/v1/product_types/6/products.json",
        "https://api.bsale.cl/v1/product_types/15/products.json"
    ]

    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    all_products = {}
    total_products_count = 0

    for api_url in api_urls:
        products, total_products = fetch_product_type(api_urls, access_token)
        for product in products:
            if "id" in product:
                all_products[product["id"]] = product  # Utilizar el ID como clave en un diccionario

        total_products_count += total_products

    print(f"Total de Productos Recuperados: {total_products_count}")

    save_typeProducts_to_mongodb