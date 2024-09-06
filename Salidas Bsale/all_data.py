import requests
import datetime
import xml.etree.ElementTree as ET
import time
from pymongo import MongoClient, errors

def fetch_data(api_url, access_token, limit=1):
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
                    "emissionDate": datetime.datetime.fromtimestamp(item['emissionDate']),
                    "date": datetime.datetime.fromtimestamp(item['emissionDate']).strftime('%Y-%m-%d'),
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
                    "urlXml": item.get("urlXml"),
                    "amount": item.get("totalAmount", 0.0),
                    "items": [],
                    "id_product": None,
                    "name_product": None,
                    "name_type_product": None,
                    "client_name": None,
                    "typeDte_description": None,
                    "source": "invoice"
                }
                all_data.append(filtered_item)
            offset += limit

        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
            break
    
    return all_data, total_documents

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

def fetch_dte_codes(api_url, headers):
    dte_codes = {}

    while api_url:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()

            for item in data['items']:
                dte_codes[item['codeSii']] = item['name']
            api_url = data.get('next', None)
        else:
            print(f"Error al obtener DTE codes: {response.status_code}")
            break

    return dte_codes

def parse_xml_details(xml_content, dte_codes):
    try:
        root = ET.fromstring(xml_content)
        namespaces = {'sii': 'http://www.sii.cl/SiiDte'}

        typeDte_element = root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE', namespaces)
        typeDte = typeDte_element.text if typeDte_element is not None else ''
        typeDte_description = dte_codes.get(typeDte, 'Desconocido')

        date_element = root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:FchEmis', namespaces)
        date = date_element.text if date_element is not None else ''
        
        client_name_element = root.find('.//sii:Documento/sii:Encabezado/sii:Receptor/sii:RznSocRecep', namespaces)
        client_name = client_name_element.text if client_name_element is not None else ''
        
        client_activity_element = root.find('.//sii:Documento/sii:Receptor/sii:GiroRecep', namespaces)
        client_activity = client_activity_element.text if client_activity_element is not None else ''

        total_amount_element = root.find('.//sii:Documento/sii:Totales/sii:MntTotal', namespaces)
        total_amount = total_amount_element.text if total_amount_element is not None else 0

        net_amount_element = root.find('.//sii:Documento/sii:Totales/sii:MntNeto', namespaces)
        net_amount = net_amount_element.text if net_amount_element is not None else 0

        iva_element = root.find('.//sii:Documento/sii:Totales/sii:IVA', namespaces)
        iva = iva_element.text if iva_element is not None else 0

        items = []
        for item in root.findall('.//sii:Documento/sii:Detalle', namespaces):
            item_details = {
                "description": item.find('sii:NmbItem', namespaces).text if item.find('sii:NmbItem', namespaces) is not None else '',
                "quantity": item.find('sii:QtyItem', namespaces).text if item.find('sii:QtyItem', namespaces) is not None else '',
                "price": item.find('sii:PrcItem', namespaces).text if item.find('sii:PrcItem', namespaces) is not None else '',
                "total": item.find('sii:MontoItem', namespaces).text if item.find('sii:MontoItem', namespaces) is not None else ''
            }
            items.append(item_details)

        details = {
            "typeDte_description": typeDte_description,
            "date": date,
            "client_name": client_name,
            "clientActivity": client_activity,
            "totalAmount": total_amount,
            "netAmount": net_amount,
            "iva": iva,
            "items": items
        }
        
        print(f"Detalles parseados para la XML: {details}")
        
        return details
    except ET.ParseError as e:
        print(f"Error al analizar XML: {e}")
        return None
    except AttributeError as e:
        print(f"Error al encontrar un elemento en XML: {e}")
        return None

def fetch_producto(apiurl, access_token, limit=2):
    headers = {
        'access_token': access_token
    }

    products_data = []
    total_products = 0
    offset = 0

    while True:
        paginated_url = f"{apiurl}?limit={limit}&offset={offset}"
        print(f"Obteniendo Productos desde: {paginated_url}")

        response = requests.get(paginated_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            total_products += len(items)

            for item in items:
                product_type_url = item['product_type']['href']
                product_type_data = fetch_product_type(product_type_url, access_token)
                if product_type_data:
                    product_type_id = int(product_type_data['id'])
                    if product_type_id == 3:
                        item['category'] = 'DIRECTO'
                    elif product_type_id in [6, 15]:
                        item['category'] = 'INDIRECTO'
                    else:
                        item['category'] = 'UNKNOWN'
                    item['product_type_details'] = product_type_data
                
                item['source'] = 'product'
                item['id_product'] = item.get('id')
                item['name_product'] = item.get('name')
                item['name_type_product'] = product_type_data.get('name')
                item['urlXml'] = item.get('urlXml')  # Ensure URL XML is added to product data
                products_data.append(item)
            offset += limit

        else:
            print(f"Error al obtener datos de offset {offset}: {response.status_code}")
            break
    
    return products_data, total_products

def fetch_clients(api_url, access_token, limit=2):
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
                client_data.append({
                    "clientCode": item.get("code"),
                    "client_name": item.get("name"),
                    "clientActivity": item.get("activity"),
                    "source": "client"
                })
            offset += limit

        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
            break
    
    return client_data, total_clients

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

def save_all_data_to_mongodb(all_data):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    all_data_collection = db['all_data']
    
    all_data_collection.create_index([("number", 1), ("id", 1)], unique=True)

    for data in all_data:
        key = data.get("number", data.get("id"))
        if key:
            try:
                all_data_collection.update_one(
                    {"number": key if "number" in data else data.get("id")}, 
                    {"$set": data}, 
                    upsert=True
                )
                print(f"Datos con clave {key} insertados/actualizados en la base de datos.")
            except errors.DuplicateKeyError:
                print(f"Datos con clave {key} ya existen en la base de datos.")
        else:
            print(f"Datos sin clave única no se pueden insertar: {data}")

if __name__ == "__main__":
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"
    headers = {
        'access_token': access_token
    }

    all_data = []

    # Fetch and save invoices
    invoices_api_url = "https://api.bsale.cl/v1/documents.json?emissiondaterange=[1714521600,1717113599]"
    invoices, total_invoices = fetch_data(invoices_api_url, access_token)
    print(f"Total facturas recuperadas: {total_invoices}")
    all_data.extend(invoices)

    # Fetch DTE codes
    dte_codes_api_url = "https://api.bsale.cl/v1/dte_codes.json"
    dte_codes = fetch_dte_codes(dte_codes_api_url, headers)

    # Fetch and save XML details for invoices
    for invoice in all_data:
        if invoice["urlXml"]:
            xml_content = fetch_xml_data(invoice["urlXml"], headers)
            if xml_content:
                detail = parse_xml_details(xml_content, dte_codes)
                invoice.update(detail)

    # Fetch and save products
    product_api_urls = [
        "https://api.bsale.cl/v1/product_types/3/products.json",
        "https://api.bsale.cl/v1/product_types/6/products.json",
        "https://api.bsale.cl/v1/product_types/15/products.json"
    ]
    all_products = []
    total_products_count = 0

    for api_url in product_api_urls:
        products, total_products = fetch_producto(api_url, access_token)
        all_products.extend(products)
        total_products_count += total_products

    print(f"Total de Productos Recuperados: {total_products_count}")

    # Integrate product data into invoices and fetch XML details for products
    for invoice in all_data:
        for product in all_products:
            if invoice['clientCode'] == product.get('clientCode'):
                product_xml_details = {}
                if product.get('urlXml'):
                    product_xml_content = fetch_xml_data(product.get('urlXml'), headers)
                    if product_xml_content:
                        product_xml_details = parse_xml_details(product_xml_content, dte_codes)
                        product.update(product_xml_details)

                invoice.update({
                    "id_product": product.get('id_product'),
                    "name_product": product.get('name_product'),
                    "name_type_product": product.get('name_type_product'),
                    "product_xml_details": product_xml_details
                })
                break

    # Fetch and save clients
    clients_api_url = "https://api.bsale.cl/v1/clients.json"
    clients, total_clients = fetch_clients(clients_api_url, access_token)
    print(f"Total clientes recuperados: {total_clients}")

    # Integrate client data into invoices
    for invoice in all_data:
        for client in clients:
            if invoice['clientCode'] == client.get('clientCode'):
                invoice.update({
                    "client_name": client.get('client_name'),
                    "clientActivity": client.get('clientActivity')
                })
                break

    # Save all data to a single collection
    save_all_data_to_mongodb(all_data)
