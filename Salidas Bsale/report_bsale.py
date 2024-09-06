import requests
from pymongo import MongoClient
import xml.etree.ElementTree as ET
import time

# Conexión a MongoDB
client = MongoClient('localhost', 27017)
db = client['api_data']


def safe_request(url, headers, retries=3, delay=5):
    #Hace solicitudes con manejo de errores y reintentos.
    for _ in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
           
            if response.status_code == 200:
                return response
      
            else:
                print(f"Error {response.status_code} al hacer solicitud a: {url}")
      
        except requests.exceptions.RequestException as e:
            print(f"Excepción al hacer solicitud a {url}: {e}")
   
            time.sleep(delay)
   
    return None


def fetch_clientes(api_url, access_token, limit=25):
    headers = {'access_token': access_token}
    client_data, offset = [], 0
   
    while True:
        url = f"{api_url}?limit={limit}&offset={offset}"
        response = safe_request(url, headers)
      
        if response:
            items = response.json().get('items', [])
          
            if not items:
                break
         
            client_data.extend(items)
            offset += limit
       
            print(f"Obtenidos {len(items)} clientes, offset ahora en {offset}")
      
        else:
            break
   
    return client_data


def fetch_product_type(api_urls, access_token):
    headers = {'access_token': access_token}
    all_products = []
   
    for api_url in api_urls:
        response = safe_request(api_url, headers)
   
        if response:
            items = response.json().get('items', [])
            all_products.extend(items)
 
            print(f"Obtenidos {len(items)} tipos de productos desde {api_url}")
  
    return all_products


def fetch_products(api_url, access_token, limit=25):
    headers = {'access_token': access_token}
    products_data, offset = [], 0
   
    while True:
        url = f"{api_url}&limit={limit}&offset={offset}"
        response = safe_request(url, headers)
    
        if response:
            items = response.json().get('items', [])
     
            if not items:
                break
     
            products_data.extend(items)
            offset += limit
      
            print(f"Obtenidos {len(items)} productos, offset ahora en {offset}")
      
        else:
            break
   
    return products_data


def fetch_data(api_url, access_token, limit=25):
    headers = {'access_token': access_token}
    all_data, offset = [], 0
   
    while True:
        url = f"{api_url}&limit={limit}&offset={offset}"
        response = safe_request(url, headers)
   
        if response:
            items = response.json().get('items', [])
    
            if not items:
                break
    
            all_data.extend(items)
            offset += limit
      
            print(f"Obtenidos {len(items)} documentos, offset ahora en {offset}")
     
        else:
            break
   
    return all_data


def fetch_xml_data(url, headers):
    response = safe_request(url, headers)
  
    if response:
        print(f"XML obtenido desde {url}")
   
        return response.text
  
    else:
        print(f"Error obteniendo XML desde {url}")
  
        return None


def fetch_dte_codes(api_url, headers):
    dte_codes = {}
 
    while api_url:
        response = safe_request(api_url, headers)
 
        if response:
            for item in response.json().get('items', []):
                dte_codes[item['codeSii']] = item['name']
            api_url = response.json().get('next', None)
  
            print(f"Códigos DTE obtenidos, siguiente página: {api_url}")
  
        else:
            break
  
    return dte_codes


def parse_xml_details(xml_content, invoice_number, dte_codes):
    namespaces = {'sii': 'http://www.sii.cl/SiiDte'}
    root = ET.fromstring(xml_content)
   
    details = {
        "invoice_number": invoice_number,
        "number": root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:Folio', namespaces).text if root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:Folio', namespaces) is not None else '',
        "typeDte": root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE', namespaces).text if root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE', namespaces) is not None else '',
        "typeDte_description": dte_codes.get(root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE', namespaces).text, 'Desconocido') if root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE', namespaces) is not None else 'Desconocido',
        "date": root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:FchEmis', namespaces).text if root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:FchEmis', namespaces) is not None else '',
        "client_name": root.find('.//sii:Documento/sii:Encabezado/sii:Receptor/sii:RznSocRecep', namespaces).text if root.find('.//sii:Documento/sii:Encabezado/sii:Receptor/sii:RznSocRecep', namespaces) is not None else '',
        "amount": root.find('.//sii:Documento/sii:Totales/sii:MntTotal', namespaces).text if root.find('.//sii:Documento/sii:Totales/sii:MntTotal', namespaces) is not None else '',
        "items": [{"description": item.find('sii:NmbItem', namespaces).text if item.find('sii:NmbItem', namespaces) is not None else '',
                   "quantity": item.find('sii:QtyItem', namespaces).text if item.find('sii:QtyItem', namespaces) is not None else '',
                   "price": item.find('sii:PrcItem', namespaces).text if item.find('sii:PrcItem', namespaces) is not None else '',
                   "total": item.find('sii:MontoItem', namespaces).text if item.find('sii:MontoItem', namespaces) is not None else ''}
                  for item in root.findall('.//sii:Documento/sii:Detalle', namespaces)]
    }
   
    print(f"Obteniendo el detalle de la factura {invoice_number}")
   
    return details


def save_to_mongodb(data, collection_name):
    collection = db[collection_name]
   
    for item in data:
        if isinstance(item, dict) and "id" in item:
            collection.update_one({"id": item["id"]}, {"$set": item}, upsert=True)
    
            print(f"Guardando Item ID {item['id']} en {collection_name}")
   
        else:
            print(f"Elemento ignorado: {item}")


def save_xml_details_to_mongodb(xml_details):
    collection = db['xml_details']
   
    for detail in xml_details:
        collection.update_one({"invoice_number": detail["invoice_number"]}, {"$set": detail}, upsert=True)
     
        print(f"Detalle de Factura {detail['invoice_number']} guardado")


def get_invoice_xml_urls_from_db():
    collection = db['facturas_2024']
    invoices = collection.find({"urlXml": {"$exists": True, "$ne": None}}, {"number": 1, "urlXml": 1})
  
    return [{"invoice_number": invoice["number"], "url": invoice["urlXml"]} for invoice in invoices]


if __name__ == "__main__":
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"
    headers = {'access_token': access_token}

    # Recolección de datos
    clients = fetch_clientes("https://api.bsale.cl/v1/clients.json", access_token)
    save_to_mongodb(clients, "clients")

    product_types = fetch_product_type(
        ["https://api.bsale.cl/v1/product_types/3/products.json",
         "https://api.bsale.cl/v1/product_types/6/products.json",
         "https://api.bsale.cl/v1/product_types/15/products.json"], access_token)
    save_to_mongodb(product_types, "type_products")

    products = fetch_products("https://api.bsale.cl/v1/products.json?expand=product_type", access_token)
    save_to_mongodb(products, "products")

    documents = fetch_data("https://api.bsale.cl/v1/documents.json?emissiondaterange=[1717200000,1719792000]", access_token)
    save_to_mongodb(documents, "facturas_2024")

    # Obtener códigos DTE
    dte_codes = fetch_dte_codes("https://api.bsale.cl/v1/dte_codes.json", headers)

    # Procesar datos XML
    xml_urls = get_invoice_xml_urls_from_db()
    xml_details = []
    for xml in xml_urls:
        xml_content = fetch_xml_data(xml["url"], headers)
     
        if xml_content:  # Ensure we proceed only if XML data was successfully fetched
            details = parse_xml_details(xml_content, xml["invoice_number"], dte_codes)
            xml_details.append(details)
    
    save_xml_details_to_mongodb(xml_details)