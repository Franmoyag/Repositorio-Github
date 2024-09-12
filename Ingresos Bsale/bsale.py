import requests
from pymongo import MongoClient

# Lista de códigos SII correspondientes a facturas
factura_codes = ["30", "33", "32", "34", "46", "45", "101", "110"]

def fetch_data(api_url, access_token, limit=25):
    headers = {
        'access_token': access_token
    }
    
    response = requests.get(api_url, headers=headers)
    if response.status_code != 200:
        print(f"Error al obtener el conteo total: {response.status_code}")
        return []
    
    total_documents = response.json().get('count', 0)
    print(f"Total de documentos: {total_documents}")
    

    all_data = []
    for offset in range(0, total_documents, limit):
        paginated_url = f"{api_url}&limit={limit}&offset={offset}"
        response = requests.get(paginated_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            for item in items:
                # Filtrar solo las facturas (excluyendo las guías de despacho)
                if item.get("codeSii") in factura_codes:
                    filtered_item = {
                        "emissionDate": item.get("emissionDate") or "",
                        "month": item.get("month") or "",
                        "year": item.get("year") or "",
                        "number": item.get("number") or "",
                        "codeSii": item.get("codeSii") or "",                    
                        "clientCode": item.get("clientCode") or "",
                        "clientActivity": item.get("clientActivity") or "",
                        "netAmount": item.get("netAmount") or 0.0,
                        "iva": item.get("iva") or 0.0,
                        "ivaAmount": item.get("ivaAmount") or 0.0,
                        "totalAmount": item.get("totalAmount") or 0.0,
                        "bookType": item.get("bookType") or "",
                        "urlPdf": item.get("urlPdf") or "",
                        "urlXml": item.get("urlXml") or ""
                    }
                    all_data.append(filtered_item)
        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
    
    return all_data

def save_to_mongo(data, db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    
    # Insertar o actualizar datos en MongoDB
    for item in data:
        print(f"Insertando/actualizando en MongoDB: {item}")
        collection.update_one(
            {"number": item['number']}, 
            {"$set": item}, 
            upsert=True
        )
    
    print("Datos insertados/actualizados en la base de datos MongoDB.")
    client.close()

api_url = "https://api.bsale.cl/v1/third_party_documents.json?year=2024"
access_token = "e57d148002d91e58f152bece58d810e50bc84286"

# Fetch data from API
data = fetch_data(api_url, access_token)

# Save data to MongoDB
if data:
    save_to_mongo(data, db_name='compras_bsale', collection_name='bsale_documents')
else:
    print("No se obtuvieron datos para guardar en MongoDB.")

print("Proceso Finalizado.....")
