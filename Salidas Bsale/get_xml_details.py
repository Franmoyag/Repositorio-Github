import requests
import xml.etree.ElementTree as ET
import time
from pymongo import MongoClient, errors



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
            api_url = data.get('next', None)  # Obtener la URL de la siguiente página, si existe

        else:
            print(f"Error al obtener DTE codes: {response.status_code}")
            break

    return dte_codes



def parse_xml_details(xml_content, invoice_number):
    try:
        root = ET.fromstring(xml_content)
        
        # Definir el espacio de nombres
        namespaces = {'sii': 'http://www.sii.cl/SiiDte'}
        
        # Navegar a través de la estructura del XML para encontrar los elementos necesarios
        typeDte_element = root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:TipoDTE',namespaces)
        typeDte = typeDte_element.text if typeDte_element is not None else ''
        typeDte_description = dte_codes.get(typeDte, 'Desconocido')

        date_element = root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:FchEmis', namespaces)
        date = date_element.text if date_element is not None else ''
        
        client_name_element = root.find('.//sii:Documento/sii:Encabezado/sii:Receptor/sii:RznSocRecep', namespaces)
        client_name = client_name_element.text if client_name_element is not None else ''
        
        amount_element = root.find('.//sii:Documento/sii:Totales/sii:MntTotal', namespaces)
        amount = amount_element.text if amount_element is not None else ''

        number_element = root.find('.//sii:Documento/sii:Encabezado/sii:IdDoc/sii:Folio',namespaces)
        number = number_element.text if number_element is not None else ''

        


        items = []
        for item in root.findall('.//sii:Documento/sii:Detalle', namespaces):
            description_element = item.find('sii:NmbItem', namespaces)
            quantity_element = item.find('sii:QtyItem', namespaces)
            price_element = item.find('sii:PrcItem', namespaces)
            total_element = item.find('sii:MontoItem', namespaces)
            
            item_details = {
                "description": description_element.text if description_element is not None else '',
                "quantity": quantity_element.text if quantity_element is not None else '',
                "price": price_element.text if price_element is not None else '',
                "total": total_element.text if total_element is not None else ''
            }
            items.append(item_details)

        details = {
            "invoice_number": invoice_number,
            "number": number,
            "typeDte": typeDte,
            "typeDte_description": typeDte_description,
            "date": date,
            "client_name": client_name,
            "amount": amount,
            "items": items
        }
        
        # Imprimir detalles para depuración
        print(f"Detalles parseados para la factura {invoice_number}: {details}")
        
        return details
    
    except ET.ParseError as e:
        print(f"Error al analizar XML: {e}")

        return None
    
    except AttributeError as e:
        print(f"Error al encontrar un elemento en XML: {e}")
        
        return None



def save_xml_details_to_mongodb(xml_details):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    xml_collection = db['xml_details']  # Nueva colección para almacenar detalles de XML
    
    # Crear índice único en el campo 'invoice_number' para evitar duplicados
    xml_collection.create_index("invoice_number", unique=True)

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



def get_invoice_xml_urls_from_db():
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    collection = db['facturas_2024']
    
    # Obtener todas las facturas que tienen una URL de XML
    invoices = collection.find({"urlXml": {"$exists": True, "$ne": None}}, {"number": 1, "urlXml": 1})
    xml_urls = [{"invoice_number": invoice["number"], "url": invoice["urlXml"]} for invoice in invoices]
    return xml_urls



if __name__ == "__main__":
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"
    headers = {
        'access_token': access_token
    }


    api_url = "https://api.bsale.cl/v1/dte_codes.json"
    dte_codes = fetch_dte_codes(api_url, headers)


    xml_urls = get_invoice_xml_urls_from_db()
    xml_details = []
    for xml in xml_urls:
        xml_content = fetch_xml_data(xml["url"], headers)
        if xml_content:
            detail = parse_xml_details(xml_content, xml["invoice_number"])
            if detail:
                xml_details.append(detail)
                
    
    save_xml_details_to_mongodb(xml_details)
