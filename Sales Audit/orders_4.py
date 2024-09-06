from sshtunnel import SSHTunnelForwarder
import pymongo
import threading
import time
import sys
from datetime import datetime, timedelta

def loading_animation(stop_event):
    chars = "/—\\|"
    while not stop_event.is_set():
        for char in chars:
            sys.stdout.write('\r' + 'Cargando... ' + char)
            time.sleep(0.1)
            sys.stdout.flush()
    sys.stdout.write('\rProceso completado!          \n')



MONGO_HOST_LOCAL = "localhost"
MONGO_DB_LOCAL = "production_expenses" # Base de Datos.... Modificar Local o API.


# Configuración de la conexión SSH para MongoDB remoto
MONGO_HOST = "67.205.176.183"
MONGO_DB = "jengibre"
MONGO_USER = ""
MONGO_PASS = ""

SERVER_SSH_PORT = 22
SSH_USERNAME = "root"
SSH_PASSWORD = "84tu38PzuHLP"

def connect_mongo_remote():
    server_mongo = SSHTunnelForwarder(
        (MONGO_HOST, SERVER_SSH_PORT),
        ssh_username=SSH_USERNAME,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=('127.0.0.1', 27017)
    )
    
    server_mongo.start()
    client_mongo = pymongo.MongoClient('127.0.0.1', server_mongo.local_bind_port)
    db_origin = client_mongo[MONGO_DB]
    collection_mongo_remote = db_origin['orders']

    return server_mongo, client_mongo, collection_mongo_remote


def connect_mongo_local():
    client_mongo_local = pymongo.MongoClient(MONGO_HOST_LOCAL, 27017)
    db_local = client_mongo_local[MONGO_DB_LOCAL]
    collection_mongo_local = db_local['orders_4']
    
    return client_mongo_local, collection_mongo_local


def main():
    stop_event = threading.Event()  # Evento para detener el hilo de la animación
    loader_thread = threading.Thread(target=loading_animation, args=(stop_event,))  # Hilo para la animación

    try:
        # Inicia la animación de carga
        loader_thread.start()

        # Conexión a MongoDB remoto
        server_mongo, client_mongo, collection_mongo_remote = connect_mongo_remote()

        # Conexión a MongoDB local
        client_mongo_local, collection_mongo_local = connect_mongo_local()


        # Fechas (Ayer y Hoy)
        yesterday = datetime.now() - timedelta(days=1)
        today = datetime.now()
        
        start = str(yesterday.date())+'T12:00:00'
        end = str(today.date())+'T06:59:59'
      

        # Recuperar documentos desde MongoDB remoto
        documents = list(collection_mongo_remote.find({
                            'datetime': {
                                '$gte': start,
                                '$lte': end
                           }
                        }))

        

        # Inserción de los datos obtenidos en MongoDB local sin modificar el origen
        extract = "01:00 HRS"

        for document in documents:
            document["extract"] = extract # #Se agrega columna para incluir hora de extraccion
            
            collection_mongo_local.replace_one({"_id": document["_id"]}, document, upsert=True)
            print(f"Documento con _id {document['_id']} insertado/reemplazado en MongoDB local.")

        # Cerrar la conexión a MongoDB remoto
        client_mongo.close()
        server_mongo.stop()

        # Cerrar la conexión a MongoDB local
        client_mongo_local.close()

    finally:
        stop_event.set()  # Detiene la animación de carga
        loader_thread.join()  # Espera a que el hilo de la animación termine

if __name__ == "__main__":
    main()
