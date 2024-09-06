import requests
from pymongo import MongoClient

def fetch_data(api_url, access_token, limit=25):
    headers = {
        'access_token': access_token
    }

    all_data = []
    total_documents = 0
    offset = 0

    while True:
        paginated_url = f"{api_url}&limit={limit}&offset={offset}"
        print(f"Obteniendo datos desde: {paginated_url}")
        
        response = requests.get(paginated_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            total_documents += len(items)

            for item in items:
                filtered_item = {
                    "emissionDate": item.get("emissionDate"),
                    "number": item.get("number"),
                    "codeSii": item.get("codeSii"),
                    "document_type": item.get("document_type"),
                    "clientCode": item.get("clientCode"),
                    "clientActivity": item.get("clientActivity"),
                    "netAmount": item.get("netAmount", 0.0),
                    "iva": item.get("iva", 0.0),
                    "ivaAmount": item.get("ivaAmount", 0.0),
                    "totalAmount": item.get("totalAmount", 0.0),
                    "bookType": item.get("bookType"),
                    "urlPdf": item.get("urlPdf"),
                    "urlXml": item.get("urlXml")
                }
                all_data.append(filtered_item)
            offset += limit

        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}, {response.text}")
            break
    
    return all_data, total_documents

def fetch_xml_data(url, headers):
    if not url:
        return None
    
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.text
    else:
        print(f"Error al obtener XML desde {url}: {response.status_code}")
        return None

def save_to_mongodb(data):
    try:
        client = MongoClient('localhost', 27017)
        db = client['api_data']
        collection = db['facturas_2024']
        
        for item in data:
            collection.update_one(
                {'number': item['number']},  # Filtro por número
                {'$set': item},              # Datos a actualizar
                upsert=True                  # Insertar si no existe
            )
        print("Datos insertados/actualizados en MongoDB.")
    except Exception as e:
        print("Error al guardar datos en MongoDB:", e)

if __name__ == "__main__":
    api_url = "https://api.bsale.cl/v1/documents.json?emissiondaterange=[1717200000,1719792000]" #Facturas Junio
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    data, total_documents = fetch_data(api_url, access_token)
    print(f"Total documentos recuperados: {total_documents}")
    
    for doc in data:
        print(doc)

    save_to_mongodb(data)
