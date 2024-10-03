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
MONGO_DB = "production_expenses"
MONGO_COLLECTION = "sales_report"
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

    # POSTGRES CLIENT CONNECT
    conn = psycopg2.connect(
            dbname=POSTGRES_DB, 
            user=POSTGRES_USER, 
            password=POSTGRES_PASS, 
            host='127.0.0.1', 
            port=server_postgres.local_bind_port)
        
    cursor = conn.cursor()

    # REFRESHING MATERIALIZED VIEWS FOR SALE REPORT 
    cursor.execute("REFRESH MATERIALIZED VIEW z_workingday")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders_customers")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders_c")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders_delivery")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders_products")
    cursor.execute("REFRESH MATERIALIZED VIEW z_orders_payments")
    cursor.execute("REFRESH MATERIALIZED VIEW z_sales_report")
    cursor.execute("REFRESH MATERIALIZED VIEW z_ranking_products")
    cursor.execute("REFRESH MATERIALIZED VIEW z_sales_report_payments")
    cursor.execute("REFRESH MATERIALIZED VIEW z_sales_report_tips")

    query = "SELECT * FROM z_sales_report"

    try:
        # Ejecutar la consulta
        cursor.execute(query, (yesterday_date,))
        # Obtener todos los resultados
        results = cursor.fetchall()

        # Obtener los nombres de las columnas
        colnames = [desc[0] for desc in cursor.description]
        print("Column names:", colnames)

        # Conectar a MongoDB
        mongo_client = pymongo.MongoClient(MONGO_HOST)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]

        # Verificar si el índice 'working_day_1' existe antes de intentar eliminarlo
        index_name = 'working_day_1'
        indexes = mongo_collection.index_information()
        if index_name in indexes:
            mongo_collection.drop_index(index_name)
            print(f"Índice '{index_name}' eliminado.")

        # Crear un índice único compuesto en 'working_day' y 'store_type'
        mongo_collection.create_index([("working_day", pymongo.ASCENDING), ("store_type", pymongo.ASCENDING)], unique=True)
        print("Índice compuesto 'working_day' y 'store_type' creado.")

        # Preparar los datos para insertar en MongoDB
        for row in results:
            row = convert_data_types(row)   # Convertir fechas a datetime
            doc = {colnames[i]: row[i] for i in range(len(colnames))}
            
            if 'working_day' in doc and 'store_type' in doc:
                try:
                    # Comprobar si ya existe un registro con el mismo 'working_day' y 'store_type'
                    existing_doc = mongo_collection.find_one({"working_day": doc["working_day"], "store_type": doc["store_type"]})
                    
                    if existing_doc:
                        # Si ya existe un registro con el mismo 'working_day' y 'store_type', actualizarlo
                        mongo_collection.update_one(
                            {"working_day": doc["working_day"], "store_type": doc["store_type"]},
                            {"$set": doc}
                        )
                        print(f"Documento con working_day {doc['working_day']} y store_type {doc['store_type']} actualizado en la Base de Datos.")
                    else:
                        # Si no existe un registro con el mismo 'working_day' y 'store_type', insertar uno nuevo
                        mongo_collection.insert_one(doc)
                        print(f"Nuevo documento insertado con working_day {doc['working_day']} y store_type {doc['store_type']}.")
                
                except Exception as e:
                    print(f"Error al insertar/actualizar el documento con working_day {doc['working_day']} y store_type {doc['store_type']}: {str(e)}")
            else:
                print(f"El documento no tiene un campo 'working_day' o 'store_type': {doc}")


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
