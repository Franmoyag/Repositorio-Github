import requests
from pymongo import MongoClient, errors



def fetch_clients(api_url, access_token, limit=25):
    headers = {
        'access_token': access_token
    }

    client_data = []
    total_clients = 0
    offset = 0
    while True:
        paginated_url = f"{api_url}?limit={limit}&offset={offset}"
        print(f"Fetching data with URL: {paginated_url}")

        response = requests.get(paginated_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            total_clients += len(items)

            for item in items:
                client_data.append(item)
            offset += limit

        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
            break
    
    return client_data, total_clients



def save_clients_to_mongodb(client_data):
    client = MongoClient('localhost', 27017)
    db = client['api_data']

    client_collection = db['clients']  # Nueva colección para almacenar clientes
    
    # Crear índice único en el campo 'id' para evitar duplicados
    client_collection.create_index("id", unique=True)

    for client in client_data:
        try:
            client_collection.update_one(
                {"id": client["id"]}, 
                {"$set": client}, 
                upsert=True
            )
            print(f"Cliente con ID {client['id']} insertado/actualizado en la base de datos.")
        except errors.DuplicateKeyError:
            
            print(f"Cliente con ID {client['id']} ya existe en la base de datos.")

if __name__ == "__main__":
    api_url = "https://api.bsale.cl/v1/clients.json"
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    clients, total_clients = fetch_clients(api_url, access_token)
    print(f"Total clientes recuperados: {total_clients}")
    for client in clients:
        print(client)

    save_clients_to_mongodb(clients)
