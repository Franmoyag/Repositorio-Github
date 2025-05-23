from sshtunnel import SSHTunnelForwarder
import pymongo 
from datetime import datetime, date, timedelta
import psycopg2
from decimal import Decimal



# THIS IS A SERVER PARAMETERS 

SERVER_SSH_PORT_POSTGRES = 22
SSH_USERNAME_POSTGRES = "root"
SSH_PASSWORD_POSTGRES = "84tu38PzuHLP"

POSTGRES_HOST = "67.207.93.19"
POSTGRES_DB = "jengibre"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "j4mdl9to5sd"

# INTERNAL SERVER PARAMETERS

MONGO_HOST = "127.0.0.1"
MONGO_DB = "data_bsale" #Base de Datos segun necesidad.
MONGO_COLLECTION = "wasabi_expenses"
MONGO_USER = ""
MONGO_PASS = ""


def convert_data_types(row):
    """Convert data types in the row to types compatible with MongoDB"""
    converted_row = []
    for item in row:
        if isinstance(item, date):
            converted_row.append(datetime.combine(item, datetime.min.time()))
        elif isinstance(item, Decimal):
            converted_row.append(float(item))
        else:
            converted_row.append(item)
    return converted_row

def fetch_and_store_gp():

    yesterday = datetime.now() - timedelta(1)
    yesterday_date = yesterday.strftime('%Y-%m-%d')

    server_postgres = SSHTunnelForwarder(
        POSTGRES_HOST,
        ssh_username=SSH_USERNAME_POSTGRES,
        ssh_password=SSH_PASSWORD_POSTGRES,
        remote_bind_address=('127.0.0.1', 5432)  
    )
    server_postgres.start()

    #POSTGRES CLIENT CONNECT
    conn = psycopg2.connect(
            dbname=POSTGRES_DB, 
            user=POSTGRES_USER, 
            password=POSTGRES_PASS, 
            host='127.0.0.1', 
            port=server_postgres.local_bind_port)
        
    cursor = conn.cursor()

    query = "SELECT * FROM tabla_gp WHERE concepto = 'Compras' AND fecha = %s"

    try:
        # Ejecutar la consulta
        cursor.execute(query, (yesterday_date,))
        # Obtener todos los resultados
        results = cursor.fetchall()

        # Obtener los nombres de las columnas
        colnames = [desc[0] for desc in cursor.description]

        # Conectar a MongoDB
        mongo_client = pymongo.MongoClient(MONGO_HOST)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]

        # Preparar los datos para insertar en MongoDB
        documents = []
        for row in results:
            row = convert_data_types(row)   # Convertir fechas a datetime
            doc = {colnames[i]: row[i] for i in range(len(colnames))}
            documents.append(doc)

        # Insertar los documentos en la colección de MongoDB
        if documents:
            mongo_collection.insert_many(documents)
            print(f"Insertados {len(documents)} documentos en la colección de MongoDB.")
        else:
            print("No se encontraron datos para insertar.")
        
    except Exception as e:
        print(f"Error al ejecutar la consulta o al insertar en MongoDB: {e}")
    finally:
        # Cerrar la conexión
        cursor.close()
        conn.close()
        if 'mongo_client' in locals():
            mongo_client.close()
        server_postgres.close()

if __name__ == "__main__":
    fetch_and_store_gp()