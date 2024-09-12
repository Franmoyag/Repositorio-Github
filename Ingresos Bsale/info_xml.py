import requests
from pymongo import MongoClient
import xml.etree.ElementTree as ET
import time

# Lista de códigos SII correspondientes a facturas
factura_codes = ["30", "33", "32", "34", "46", "45", "101", "110"]



def fetch_and_parse_xml(url_xml, retries=3, delay=5):
    if not url_xml:
        print("URL vacía, omitiendo...")
        return []
    
    for attempt in range(retries):
        try:
            response = requests.get(url_xml)
            response.raise_for_status()  # Excepción si la solicitud fue incorrecta
            xml_content = response.content
            root = ET.fromstring(xml_content)
            
            details_list = []
            detalles = root.findall('.//{http://www.sii.cl/SiiDte}Detalle')


            # Receptor: información opcional
            receptor = root.find('.//{http://www.sii.cl/SiiDte}Receptor')
            receptor_info = {
                "RUTRecep": receptor.find('{http://www.sii.cl/SiiDte}RUTRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}RUTRecep') is not None else "",
                "RznSocRecep": receptor.find('{http://www.sii.cl/SiiDte}RznSocRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}RznSocRecep') is not None else "",
                "GiroRecep": receptor.find('{http://www.sii.cl/SiiDte}GiroRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}GiroRecep') is not None else "",
                "Contacto": receptor.find('{http://www.sii.cl/SiiDte}Contacto').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}Contacto') is not None else "",
                "DirRecep": receptor.find('{http://www.sii.cl/SiiDte}DirRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}DirRecep') is not None else "",
                "CmnaRecep": receptor.find('{http://www.sii.cl/SiiDte}CmnaRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}CmnaRecep') is not None else "",
                "CiudadRecep": receptor.find('{http://www.sii.cl/SiiDte}CiudadRecep').text if receptor is not None and receptor.find('{http://www.sii.cl/SiiDte}CiudadRecep') is not None else "",
            }

             # Emisor: información opcional
            emisor = root.find('.//{http://www.sii.cl/SiiDte}Emisor')
            emisor_info = {
                "RUTEmi": emisor.find('{http://www.sii.cl/SiiDte}RUTEmisor').text if emisor is not None and emisor.find('{http://www.sii.cl/SiiDte}RUTEmisor') is not None else "",
                "RznSocEmi": emisor.find('{http://www.sii.cl/SiiDte}RznSoc').text if emisor is not None and emisor.find('{http://www.sii.cl/SiiDte}RznSoc') is not None else "",
                "GiroEmi": emisor.find('{http://www.sii.cl/SiiDte}GiroEmis').text if emisor is not None and emisor.find('{http://www.sii.cl/SiiDte}GiroEmis') is not None else "",
                "Vendedor": emisor.find('{http://www.sii.cl/SiiDte}CdgVendedor').text if emisor is not None and emisor.find('{http://www.sii.cl/SiiDte}CdgVendedor') is not None else "",
            }


            # Transporte: información opcional
            transporte = root.find('.//{http://www.sii.cl/SiiDte}Transporte')
            transporte_info = {
                "DirDest": transporte.find('{http://www.sii.cl/SiiDte}DirDest').text if transporte is not None and transporte.find('{http://www.sii.cl/SiiDte}DirDest') is not None else "",
                "CmnaDest": transporte.find('{http://www.sii.cl/SiiDte}CmnaDest').text if transporte is not None and transporte.find('{http://www.sii.cl/SiiDte}CmnaDest') is not None else "",
                "CiudadDest": transporte.find('{http://www.sii.cl/SiiDte}CiudadDest').text if transporte is not None and transporte.find('{http://www.sii.cl/SiiDte}CiudadDest') is not None else "",
            }


            # IdDoc: información opcional
            id_doc = root.find('.//{http://www.sii.cl/SiiDte}IdDoc')
            id_doc_info = {
                "Folio": id_doc.find('{http://www.sii.cl/SiiDte}Folio').text if id_doc is not None and id_doc.find('{http://www.sii.cl/SiiDte}Folio') is not None else "",
                "FchEmis": id_doc.find('{http://www.sii.cl/SiiDte}FchEmis').text if id_doc is not None and id_doc.find('{http://www.sii.cl/SiiDte}FchEmis') is not None else "",
            }


            for detalle in detalles:
                detail_info = {
                    "NroLinDet": detalle.find('{http://www.sii.cl/SiiDte}NroLinDet').text if detalle.find('{http://www.sii.cl/SiiDte}NroLinDet') is not None else 0,
                    "TpoCodigo": detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}TpoCodigo').text if detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}TpoCodigo') is not None else "",
                    "VlrCodigo": detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}VlrCodigo').text if detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}VlrCodigo') is not None else "",
                    "NmbItem": detalle.find('{http://www.sii.cl/SiiDte}NmbItem').text if detalle.find('{http://www.sii.cl/SiiDte}NmbItem') is not None else "",
                    "QtyItem": detalle.find('{http://www.sii.cl/SiiDte}QtyItem').text if detalle.find('{http://www.sii.cl/SiiDte}QtyItem') is not None else 0.0,
                    "PrcItem": detalle.find('{http://www.sii.cl/SiiDte}PrcItem').text if detalle.find('{http://www.sii.cl/SiiDte}PrcItem') is not None else 0.0,
                    "MontoItem": detalle.find('{http://www.sii.cl/SiiDte}MontoItem').text if detalle.find('{http://www.sii.cl/SiiDte}MontoItem') is not None else 0.0,
                    **receptor_info,
                    **transporte_info,
                    **id_doc_info,
                    **emisor_info
                }

                details_list.append(detail_info)

            return details_list
        
        except requests.exceptions.RequestException as e:
            print(f"Error al descargar el XML: {e} - URL: {url_xml}")

            if attempt < retries - 1:
                print(f"Reintentando en {delay} segundos...")
                time.sleep(delay)
            else:
                return []
            
        except ET.ParseError as e:
            print(f"Error al parsear el XML: {e}")
            return []


# Guardar detalles en MongoDB
def save_details_to_mongo():
    client = MongoClient('localhost', 27017)
    db = client['compras_bsale']
    collection_bsale = db['bsale_documents']
    collection_details = db['bsale_details']

    # Leer los IDs y URLs desde la colección bsale y filtrar solo facturas (codeSii)
    records = collection_bsale.find(
        {"codeSii": {"$in": factura_codes}},  # Filtrar solo facturas
        {"number": 1, "urlXml": 1}
    )

    for record in records:
        number_doc = record["number"]
        urlXml = record.get("urlXml", "")

        if not urlXml:
            print(f"Documento {number_doc} tiene URL vacía, omitiendo...")
            continue

        xml_details = fetch_and_parse_xml(urlXml)

        if not xml_details:
            print(f"No se encontraron detalles para Documento {number_doc}, omitiendo...")
            continue

        for detail in xml_details:
            detail['number'] = number_doc
            print(f"Insertando/actualizando en MongoDB: {detail}")

            collection_details.update_one(
                {"number": number_doc, "NroLinDet": detail['NroLinDet']},
                {"$set": detail},
                upsert=True
            )

    print("Detalles insertados/actualizados en la base de datos MongoDB.")
    client.close()

# Llamada a la función para guardar en MongoDB
save_details_to_mongo()

print("Proceso Finalizado.....")
