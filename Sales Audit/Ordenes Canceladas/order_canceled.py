import pymongo
import psycopg2
import json
import threading
import time
import sys


def loading_animation(stop_event):
    chars = "/—\\|"
    while not stop_event.is_set():
        for char in chars:
            sys.stdout.write('\r' + 'Cargando... ' + char)
            time.sleep(0.1)
            sys.stdout.flush()
    sys.stdout.write('\rProceso completado!          \n')

# Conexión a MongoDB en localhost
MONGO_HOST = "localhost"
MONGO_DB = "data_bsale"

client_mongo = pymongo.MongoClient(MONGO_HOST, 27017)
db_origin = client_mongo[MONGO_DB]
collection_mongo = db_origin['ordes_canceled']

# Conexión a Postgres
POSTGRES_HOST = "67.207.93.19"
POSTGRES_DB = "jengibre"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "j4mdl9to5sd"

conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS, host=POSTGRES_HOST, port=5432)
cur = conn.cursor()

# Modifica la consulta para obtener datos de la tabla "jornadas"
cur.execute("SELECT * FROM pedidos WHERE estado = 'CANCELED' AND fecha >= '2024-08-01'")
rows = cur.fetchall()

# Opcional: obtener los nombres de las columnas para construir documentos de MongoDB
colnames = [desc[0] for desc in cur.description]

# Inserción de los datos obtenidos en MongoDB, reemplazando si el 'id' ya existe
for row in rows:
    document = dict(zip(colnames, row))
    collection_mongo.replace_one({"id": document["id"]}, document, upsert=True)
    print(f"Documento con id {document['id']} insertado/reemplazado.")

cur.close()
conn.close()
client_mongo.close()

def main():
    stop_event = threading.Event()  # Evento para detener el hilo de la animación
    loader_thread = threading.Thread(target=loading_animation, args=(stop_event,))  # Hilo para la animación

    try:
        # Aquí inicia la animación de carga
        loader_thread.start()

        # Todo tu proceso actual aquí...
        # Incluye la conexión a MongoDB, la recuperación de documentos, la inserción en PostgreSQL, etc.

    finally:
        stop_event.set()  # Detiene la animación de carga
        loader_thread.join()  # Espera a que el hilo de la animación termine

if __name__ == "__main__":
    main()
