import requests
import mysql.connector
from mysql.connector import Error
import xml.etree.ElementTree as ET
import time



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

            for detalle in detalles:
                nro_lin_det = detalle.find('{http://www.sii.cl/SiiDte}NroLinDet')
                tpo_codigo = detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}TpoCodigo')
                vlr_codigo = detalle.find('{http://www.sii.cl/SiiDte}CdgItem/{http://www.sii.cl/SiiDte}VlrCodigo')
                nmb_item = detalle.find('{http://www.sii.cl/SiiDte}NmbItem')
                qty_item = detalle.find('{http://www.sii.cl/SiiDte}QtyItem')
                prc_item = detalle.find('{http://www.sii.cl/SiiDte}PrcItem')
                monto_item = detalle.find('{http://www.sii.cl/SiiDte}MontoItem')
                
                detail_info = {
                    "NroLinDet": nro_lin_det.text if nro_lin_det is not None else 0,
                    "TpoCodigo": tpo_codigo.text if tpo_codigo is not None else "",
                    "VlrCodigo": vlr_codigo.text if vlr_codigo is not None else "",
                    "NmbItem": nmb_item.text if nmb_item is not None else "",
                    "QtyItem": qty_item.text if qty_item is not None else 0.0,
                    "PrcItem": prc_item.text if prc_item is not None else 0.0,
                    "MontoItem": monto_item.text if monto_item is not None else 0.0
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




def save_details_to_mysql(host, database, user, password):
    connection = None

    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS bsale_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                bsale_id INT,
                NroLinDet INT,
                TpoCodigo VARCHAR(255),
                VlrCodigo VARCHAR(255),
                NmbItem VARCHAR(255),
                QtyItem DECIMAL(10, 2),
                PrcItem DECIMAL(10, 2),
                MontoItem DECIMAL(10, 2),
                UNIQUE(bsale_id, NroLinDet),
                FOREIGN KEY (bsale_id) REFERENCES bsale(id)
            )
            """)
            
            detail_insert_query = """
            INSERT INTO bsale_details (
                bsale_id, NroLinDet, TpoCodigo, VlrCodigo, NmbItem, QtyItem, PrcItem, MontoItem
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                TpoCodigo=VALUES(TpoCodigo),
                VlrCodigo=VALUES(VlrCodigo),
                NmbItem=VALUES(NmbItem),
                QtyItem=VALUES(QtyItem),
                PrcItem=VALUES(PrcItem),
                MontoItem=VALUES(MontoItem)
            """
            

            # Leer los IDs y URLs de la tabla bsale
            cursor.execute("SELECT id, urlXml FROM bsale")
            records = cursor.fetchall()
            
            for record in records:
                bsale_id = record[0]
                urlXml = record[1]
                
                if not urlXml:
                    print(f"ID {bsale_id} tiene URL vacía, omitiendo...")
                    continue
                
                xml_details = fetch_and_parse_xml(urlXml)
                
                if not xml_details:
                    print(f"No se encontraron detalles para bsale_id {bsale_id}, omitiendo...")
                    continue
                
                for detail in xml_details:
                    #Imprimir los valores que se insertan en detalles
                    print("Insertando/actualizando en bsale_details:", (
                        bsale_id, detail['NroLinDet'], detail['TpoCodigo'], detail['VlrCodigo'], detail['NmbItem'], 
                        detail['QtyItem'], detail['PrcItem'], detail['MontoItem']
                    ))
                    
                    cursor.execute(detail_insert_query, (
                        bsale_id, detail['NroLinDet'], detail['TpoCodigo'], detail['VlrCodigo'], detail['NmbItem'], 
                        detail['QtyItem'], detail['PrcItem'], detail['MontoItem']
                    ))
                
            connection.commit()
            print("Detalles insertados/actualizados en la base de datos MySQL.")
    
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada.")
            

save_details_to_mysql(host='localhost', database='api_myst', user='api_user', password='kami.2024.sushi')

print("Proceso Finalizado.....")
