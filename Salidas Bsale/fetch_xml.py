import requests
import datetime
import time
import xml.etree.ElementTree as ET
from pymongo import MongoClient, errors



def fetch_invoices(api_url, access_token, limit=25):
    headers = {
        'access_token': access_token
    }


    all_data = []
    xml_details = []
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
                emission_date = datetime.datetime.fromtimestamp(item['emissionDate'])
                if emission_date.year == 2024 and emission_date.month == 5:
                    xml_url = item.get("urlXml")
                    xml_data = None
                    if xml_url:
                        xml_content = fetch_xml_data(xml_url, headers)
                        if xml_content:
                            xml_data = parse_xml_details(xml_content, item['number'])
                            if xml_data:
                                xml_details.append(xml_data)
                    filtered_item = {
                        "emissionDate": emission_date,
                        "number": item.get("number"),
                        "codeSii": item.get("codeSii"),
                        "clientCode": item.get("clientCode"),
                        "clientActivity": item.get("clientActivity"),
                        "netAmount": item.get("netAmount", 0.0),
                        "iva": item.get("iva", 0.0),
                        "ivaAmount": item.get("ivaAmount", 0.0),
                        "totalAmount": item.get("totalAmount", 0.0),
                        "bookType": item.get("bookType"),
                        "urlPdf": item.get("urlPdf"),
                        "urlXml": xml_url,
                        "xmlData": xml_data
                    }
                    all_data.append(filtered_item)
            offset += limit

        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
            break
    
    return all_data, xml_details, total_documents




def fetch_xml_data(url, headers, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return response.text
            
            else:
                print(f"Error al obtener XML desde {url}: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Exception al obtener XML desde {url}: {e}")

        if attempt < retries - 1:
            print(f"Reintentando en {delay} segundos...")
            time.sleep(delay)

    return None




def parse_xml_details(xml_content, invoice_number):
    try:
        root = ET.fromstring(xml_content)
        details = {
            "invoice_number": invoice_number,
        }
        return details
    

    except ET.ParseError as e:
        print(f"Error al analizar XML: {e}")
        return None



def save_invoices_to_mongodb(data, xml_details):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    collection = db['facturas_2024']
    xml_collection = db['xml_details']  # Nueva colección para almacenar detalles de XML
    
    # Crear índice único en el campo 'number' para evitar duplicados
    collection.create_index("number", unique=True)
    xml_collection.create_index("invoice_number", unique=True)  # Índice único para detalles de XML

    for document in data:
        try:
            collection.update_one(
                {"number": document["number"]}, 
                {"$set": document}, 
                upsert=True
            )
            print(f"Documento con número {document['number']} insertado/actualizado en la base de datos.")

        except errors.DuplicateKeyError:
            print(f"Documento con número {document['number']} ya existe en la base de datos.")

    for detail in xml_details:
        try:
            xml_collection.update_one(
                {"invoice_number": detail["invoice_number"]}, 
                {"$set": detail}, 
                upsert=True
            )
            print(f"Detalles XML para la factura {detail['invoice_number']} insertados/actualizados en la base de datos.")

        except errors.DuplicateKeyError:
            print(f"Detalles XML para la factura {detail['invoice_number']} ya existen en la base de datos.")



if __name__ == "__main__":
    api_url = "https://api.bsale.cl/v1/documents.json?emissiondaterange=[1714521600,1717113599]"
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    invoices, xml_details, total_documents = fetch_invoices(api_url, access_token)
    print(f"Total documentos recuperados: {total_documents}")
    for doc in invoices:
        print(doc)



    save_invoices_to_mongodb(invoices, xml_details)
