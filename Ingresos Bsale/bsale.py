import requests
import mysql.connector
from mysql.connector import Error

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
                filtered_item = {
                    "emissionDate": item.get ("emissionDate") or "",
                    "month": item.get ("month") or "",
                    "year": item.get ("year") or "",
                    "number": item.get ("number") or "",
                    "codeSii": item.get ("codeSii") or "",                    
                    "clientCode": item.get ("clientCode") or "",
                    "clientActivity": item.get ("clientActivity") or "",
                    "netAmount": item.get ("netAmount") or 0.0,
                    "iva": item.get ("iva") or 0.0,
                    "ivaAmount": item.get ("ivaAmount") or 0.0,
                    "totalAmount": item.get ("totalAmount") or 0.0,
                    "bookType": item.get ("bookType") or "",
                    "urlPdf": item.get ("urlPdf") or "",
                    "urlXml": item.get ("urlXml") or ""                    
                }
                all_data.append(filtered_item)
                
        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
    
    return all_data




def save_to_mysql(data, host, database, user, password):
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

            cursor.execute ("SET FOREIGN_KEY_CHECKS = 0")

            cursor.execute ("TRUNCATE TABLE bsale_details")
            cursor.execute ("TRUNCATE TABLE bsale")

            cursor.execute ("SET FOREIGN_KEY_CHECKS = 1")

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS bsale (
                id INT AUTO_INCREMENT PRIMARY KEY,
                emissionDate VARCHAR(255),
                month VARCHAR(255),
                year VARCHAR(255),
                number VARCHAR(255),
                codeSii VARCHAR(255) UNIQUE,
                clientCode VARCHAR(255),
                clientActivity VARCHAR(255),
                netAmount DECIMAL(10, 2),
                iva DECIMAL(10, 2),
                ivaAmount DECIMAL(10, 2),
                totalAmount DECIMAL(10, 2),
                bookType VARCHAR(255),
                urlPdf VARCHAR(255),
                urlXml VARCHAR(255)                
            )
            """)
            
            insert_query = """
            INSERT INTO bsale (
                emissionDate, month, year, number, codeSii, clientCode, clientActivity, netAmount, iva, ivaAmount, totalAmount, bookType, urlPdf, urlXml
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                emissionDate=VALUES(emissionDate),
                month=VALUES(month),
                year=VALUES(year),
                number=VALUES(number),
                clientCode=VALUES(clientCode),
                clientActivity=VALUES(clientActivity),
                netAmount=VALUES(netAmount),
                iva=VALUES(iva),
                ivaAmount=VALUES(ivaAmount),
                totalAmount=VALUES(totalAmount),
                bookType=VALUES(bookType),
                urlPdf=VALUES(urlPdf),
                urlXml=VALUES(urlXml)
                
            """
            
            for item in data:
                #Imprimir los valores que se insertan
                print("Insertando/actualizando en bsale:", (
                    item['emissionDate'], item['month'], item['year'], item['number'], item['codeSii'], item['clientCode'], item['clientActivity'], 
                    item['netAmount'], item['iva'], item['ivaAmount'], item['totalAmount'], item['bookType'], 
                    item['urlPdf'], item['urlXml']
                ))
                
                cursor.execute(insert_query, (
                    item['emissionDate'], item['month'], item['year'],  item['number'], item['codeSii'], item['clientCode'], item['clientActivity'], 
                    item['netAmount'], item['iva'], item['ivaAmount'], item['totalAmount'], item['bookType'], 
                    item['urlPdf'], item['urlXml']
                ))
                
            connection.commit()
            print("Datos insertados/actualizados en la base de datos MySQL.")
    
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada.")



api_url = "https://api.bsale.cl/v1/third_party_documents.json?year=2024"
access_token = "e57d148002d91e58f152bece58d810e50bc84286"  



data = fetch_data(api_url, access_token)

save_to_mysql(data, host='localhost', database='api_myst', user='api_user', password='kami.2024.sushi')

print("Proceso Finalizado.....")
