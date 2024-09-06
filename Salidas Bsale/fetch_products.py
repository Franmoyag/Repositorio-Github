import requests
from pymongo import MongoClient, errors


def fetch_producto(apiurl, access_token, limit=25):
    headers = {
        'access_token': access_token
    }

    products_data = []
    total_products = 0
    offset = 0

    while True:
        paginated_url = f"{apiurl}&limit={limit}&offset={offset}"
        print(f"Obteniendo Productos desde: {paginated_url}")

        response = requests.get(paginated_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            total_products += len(items)

            for item in items:
                products_data.append(item)
            offset += limit

        else:
            print(f"Error al obtener datos de offset {offset}: {response.status_code}")
            break  # Salir del bucle si hay un error en la solicitud

    return products_data, total_products


def save_products_to_mongodb(product_data):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    client_collection = db['products']

    # Crear índice único en el campo 'id' para evitar duplicados
    client_collection.create_index("id", unique=True)

    for product in product_data:
        try:
            client_collection.update_one(
                {"id": product["id"]},
                {"$set": product},
                upsert=True
            )
            print(f"Producto con ID {product['id']} insertado/actualizado en la Base de Datos.")
        except errors.DuplicateKeyError:
            print(f"Producto con ID {product['id']} ya existe en la Base de Datos.")
        except Exception as e:
            print(f"Error al insertar/actualizar el producto con ID {product['id']}: {str(e)}")


if __name__ == "__main__":
    api_url = "https://api.bsale.cl/v1/products.json"
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    products, total_products = fetch_producto(api_url, access_token)

    print(f"Total de Productos Recuperados: {total_products}")

    save_products_to_mongodb(products)
