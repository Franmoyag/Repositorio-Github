import requests
import pymongo
import time
from datetime import datetime, timezone, timedelta
from pymongo import errors



# Configuración
ACCESS_TOKEN = 'e57d148002d91e58f152bece58d810e50bc84286'
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'data_bsale' # Modificar Nombre de BD Segun corresponda.
LIMIT = 25  # Resultado por página.
MAX_RETRIES = 5  # Intentos máximos por error.
RETRY_DELAY = 5  # Tiempo entre reintentos (Segundos)



# Mongo Connect
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

def fetch_data(api_url, collection_name, params=None, expand=None):
    collection = db[collection_name]
    collection.create_index([('id', pymongo.ASCENDING)], unique=True)

    headers = {
        'access_token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }

    total_inserted = 0
    offset = 0

    while True:
        for attempt in range(MAX_RETRIES):
            params = params or {}
            params.update({'limit': LIMIT, 'offset': offset})

            if expand:
                params.update({'expand': expand})

            print(f"Realizando solicitud con parámetros {params}")

            response = requests.get(api_url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()

                if 'items' in data and len(data['items']) > 0:
                    print("Obteniendo datos....")

                    for item in data['items']:
                        collection.replace_one({'id': item['id']}, item, upsert=True)

                    inserted_count = len(data['items'])
                    total_inserted += inserted_count

                    print(f"Total de datos insertados/reemplazados: {total_inserted}")

                    offset += LIMIT
                    break
                else:
                    print("No se encontraron más elementos.")
                    return total_inserted
            else:
                print(f"Error al realizar la solicitud: {response.status_code} - {response.text} - Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)

        if attempt >= MAX_RETRIES:
            print(f"Max reintentos alcanzados. Error persistente: {response.status_code}")
            break

    print(f"Total final de datos Insertados/Reemplazados: {total_inserted}")


def fetch_price_list(api_urls, access_token, limit=25):
    headers = {
        'access_token': access_token
    }

    all_price_data = []
    total_price_data = 0

    for api_url in api_urls:
        offset = 0
        condition = "LISTA COSTO" if "price_lists/4" in api_url else "LISTA VENTA"

        while True:
            paginated_url = f"{api_url}&limit={limit}&offset={offset}"
            print(f"Obteniendo datos desde {paginated_url}")

            response = requests.get(paginated_url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if not items:
                    break

                total_price_data += len(items)

                for item in items:
                    item['condition'] = condition
                    all_price_data.append(item)

                offset += limit

                if len(items) < limit:
                    break

            else:
                print(f"Error al obtener datos de offset {offset}: {response.status_code}")
                break

    return all_price_data, total_price_data


def save_data_to_mongodb(collection_name, data):
    collection = db[collection_name]
    collection.create_index("id", unique=True)

    for item in data:
        try:
            collection.update_one(
                {"id": item["id"]},
                {"$set": item},
                upsert=True
            )
            print(f"Documento con ID {item['id']} Insertado/Actualizado en la Base de Datos.")
        except errors.DuplicateKeyError:
            print(f"Documento con ID {item['id']} ya existe en la Base de Datos.")
        except Exception as e:
            print(f"Error al Insertar/Actualizar el documento {item['id']}: {str(e)}")


def fetch_returns(api_url, params, headers):
    response = requests.get(api_url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        print(f"Error fetching returns: {response.status_code} - {response.text}")
        return []


def fetch_shipping(api_url, params, headers):
    response = requests.get(api_url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        print(f"Error fetching shipping: {response.status_code} - {response.text}")
        return []


def generate_date_range(start_epoch, end_epoch):
    start = datetime.fromtimestamp(int(start_epoch))
    end = datetime.fromtimestamp(int(end_epoch))
    delta = end - start
    return [start + timedelta(days=i) for i in range(delta.days + 1)]


def fetch_all_document_details(document, access_token, limit=25, max_retries=5, retry_delay=5, timeout=30):
    headers = {
        'access_token': access_token
    }
    details_url = document['details']['href']
    all_details = []

    while True:
        for attempt in range(max_retries):
            paginated_url = f"{details_url}?limit={limit}&offset={len(all_details)}"
            print(f"Obteniendo detalles desde: {paginated_url}")

            try:
                response = requests.get(paginated_url, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])
                    if not items:
                        break

                    all_details.extend(items)
                    break  # Salir del bucle de reintentos si la solicitud fue exitosa
                else:
                    print(f"Error al obtener detalles: {response.status_code}")
                    time.sleep(retry_delay)

            except requests.exceptions.Timeout:
                print(f"Timeout al obtener detalles. Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)

        else:
            print("Max reintentos alcanzados. Error persistente.")
            break

        if len(items) < limit:
            break

    document['details']['items'] = all_details
    return document


def fetch_documents_data(api_url, access_token, start_date, end_date, limit=25, max_retries=5, retry_delay=5, timeout=30):
    headers = {
        'access_token': access_token
    }

    all_document_data = []
    offset = 0

    while True:
        for attempt in range(max_retries):
            paginated_url = f"{api_url}&limit={limit}&offset={offset}&emissiondaterange=[{start_date},{end_date}]"
            print(f"Obteniendo datos desde: {paginated_url}")

            try:
                response = requests.get(paginated_url, headers=headers, timeout=timeout)
            except requests.exceptions.Timeout:
                print(f"Timeout al obtener datos de offset {offset}. Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
                continue

            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                if not items:
                    return all_document_data  # Si no hay más items, salimos de la función

                for document in items:
                    document_with_details = fetch_all_document_details(document, access_token)
                    all_document_data.append(document_with_details)

                offset += limit
                break  # Salir del bucle de reintentos si la solicitud fue exitosa

            else:
                print(f"Error al obtener datos de offset {offset}: {response.status_code}")
                time.sleep(retry_delay)
                continue  # Reintentar en caso de error

        if attempt >= max_retries:
            print(f"Max reintentos alcanzados. Error persistente en offset {offset}: {response.status_code}")
            break

        # Romper el bucle principal si no se encontraron más elementos
        if len(items) < limit:
            break

    return all_document_data


def save_document_data_to_mongodb(document_data):
    collection = db['documents']

    # Crear índice único en el campo 'id' para evitar duplicados
    collection.create_index("id", unique=True)

    for document in document_data:
        try:
            collection.update_one(
                {"id": document["id"]},
                {"$set": document},
                upsert=True
            )
            print(f"Factura con ID {document['id']} insertado/actualizado en la Base de Datos.")

        except errors.DuplicateKeyError:
            print(f"Factura con ID {document['id']} ya existe en la Base de Datos.")

        except Exception as e:
            print(f"Error al insertar/actualizar la Factura con ID {document['id']}: {str(e)}")



if __name__ == "__main__":
    # Obtener la fecha actual y las fechas de ayer y anteayer
    current_date = datetime.now(timezone.utc)
    start_date = (current_date - timedelta(days=2)).isoformat()
    end_date = (current_date - timedelta(days=1)).isoformat()

    params = {
        #'start_date': start_date,
        #'end_date': end_date

        #Fecha Manual.
        'start_date': 1717200000,
        'end_date': 1719791999
    }

    headers = {
        'access_token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }

    # Obtener y almacenar data desde las APIs
    fetch_data('https://api.bsale.cl/v1/book_types.json', 'book_types')
    fetch_data('https://api.bsale.cl/v1/dte_codes.json', 'dte_codes')
    fetch_data('https://api.bsale.cl/v1/document_types.json', 'document_types', expand='book_type')
    fetch_data('https://api.bsale.cl/v1/clients.json', 'clients', expand='payment_type,sale_condition,price_list,contacts,addresses')
    fetch_data('https://api.bsale.cl/v1/variants.json', 'variants', expand='product,costs')
    fetch_data('https://api.bsale.cl/v1/products.json', 'products',  expand='product_type,variants,product_taxes')
    fetch_data('https://api.bsale.cl/v1/users.json', 'users',  expand='office')
    fetch_data('https://api.bsale.cl/v1/payment_types.json', 'payment_types', expand='dynamic_attributes')
    fetch_data('https://api.bsale.cl/v1/offices.json', 'offices')
    fetch_data('https://api.bsale.cl/v1/taxes.json', 'taxes')
    fetch_data('https://api.bsale.cl/v1/sale_conditions.json', 'sales_condition')
    fetch_data('https://api.bsale.cl/v1/shipping_types.json', 'shipping_types')

    api_urls = [
        "https://api.bsale.cl/v1/price_lists/4/details.json?expand=variant",
        "https://api.bsale.cl/v1/price_lists/3/details.json?expand=variant"
    ]

    price_lists, total_price_data = fetch_price_list(api_urls, ACCESS_TOKEN)

    print(f"Total de Elementos Recuperados: {total_price_data}")

    save_data_to_mongodb('price_lists', price_lists)

    # Fetch and save returns
    returns = fetch_returns('https://api.bsale.cl/v1/returns.json', params, headers)
    save_data_to_mongodb('returns', returns)
    print(f"Total de Devoluciones Recuperadas: {len(returns)}")

    # Fetch and save shipping
    shipping = fetch_shipping('https://api.bsale.cl/v1/shippings.json', params, headers)
    save_data_to_mongodb('shipping', shipping)
    print(f"Total de Envíos Recuperados: {len(shipping)}")

    # Fetch and save documents al final
    api_url = "https://api.bsale.cl/v1/documents.json?expand=[client, office, sale_condition, user, coin, references, document_taxes, sellers, attributes]"
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    # Convertir start_date y end_date a enteros de época
    start_date_epoch = int(datetime.fromisoformat(start_date).timestamp())
    end_date_epoch = int(datetime.fromisoformat(end_date).timestamp())

    #date_range = generate_date_range(start_date_epoch, end_date_epoch)
    
    #Fecha Manual
    date_range = generate_date_range(1717200000, 1719791999)

    total_document_data = 0
    for date in date_range:
        start_date = int(date.timestamp())
        end_date = int((date + timedelta(days=1)).timestamp()) - 1

        document_data = fetch_documents_data(api_url, access_token, start_date, end_date)
        total_document_data += len(document_data)
        save_document_data_to_mongodb(document_data)

    print(f"Total de Facturas Recuperadas: {total_document_data}")