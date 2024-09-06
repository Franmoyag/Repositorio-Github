# -----------------------------------------------------------------------------------------------------------------

import pymongo
import psycopg2
import threading
import time
import sys
import json

def loading_animation(stop_event):
    chars = "/—\\|"
    while not stop_event.is_set():
        for char in chars:
            sys.stdout.write('\r' + 'Loading... ' + char)
            time.sleep(0.1)
            sys.stdout.flush()
    sys.stdout.write('\rProcess completed!          \n')

def translate_document(document):
    # Diccionario de traducción de claves
    translations = {
        "id": "id",
        "index": "index",
        "jornada": "jornada",
        "fecha": "datetime",
        "estado": "status",
        "tipo": "type",
        "comentario": "comment",
        "tipo_pago": "payment",
        "pagado": "pay",
        "recargo": "recargo",
        "costo_despacho": "deliveryCost",
        "repartidor": "deliveryMan",
        "cliente": "user",
        "productos": "products",
        "canal": "channel",
        "sucursal": "sucursal",
        "descuento": "descuento",
        "uuid": "uuid",
        "motivo": "motivo",
        "fecha_termino": "fecha_termino",
        "estado_anulado": "estado_anulado",
        "tags": "tags",
        "mensaje_motivo": "mensaje_motivo",
        "propina": "propina",
        "fecha_done": "fecha_done",
        "fecha_retiro": "fecha_retiro"
    }
    
    # Función recursiva para traducir claves
    def translate_key(key, value):
        if isinstance(value, dict):
            return translate_document(value)
        elif isinstance(value, list):
            return [translate_key(key, item) for item in value]
        else:
            return value

    translated_document = {}
    for key, value in document.items():
        translated_key = translations.get(key, key)
        translated_document[translated_key] = translate_key(key, value)
    
    return translated_document

def main():
    stop_event = threading.Event()  # Evento para detener el hilo de la animación
    loader_thread = threading.Thread(target=loading_animation, args=(stop_event,))  # Hilo para la animación

    try:
        # Inicia la animación de carga
        loader_thread.start()

        # Conexión a MongoDB en localhost
        MONGO_HOST = "localhost"
        MONGO_DB = "data_bsale"
        
        client_mongo = pymongo.MongoClient(MONGO_HOST, 27017)
        db_origin = client_mongo[MONGO_DB]
        collection_mongo = db_origin['orders_4']

        # Conexión a Postgres
        POSTGRES_HOST = "67.207.93.19"
        POSTGRES_DB = "jengibre"
        POSTGRES_USER = "postgres"
        POSTGRES_PASS = "j4mdl9to5sd"

        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS, host=POSTGRES_HOST, port=5432)
        cur = conn.cursor()

        cur.execute("SELECT P.* FROM pedidos P INNER JOIN jornadas J ON P.jornada = J.id WHERE J.apertura >= '2024-9-3'")

        rows = cur.fetchall()

        # Opcional: obtener los nombres de las columnas para construir documentos de MongoDB
        colnames = [desc[0] for desc in cur.description]

        # Inserción de los datos obtenidos en MongoDB
        for row in rows:
            document = dict(zip(colnames, row))
            translated_document = translate_document(document)  # Traducir el documento
            collection_mongo.insert_one(translated_document)  # Insertar directamente el documento
            print(f"Document with id {translated_document['id']} inserted.")

    finally:
        # Asegura que todas las conexiones se cierren y que la animación se detenga correctamente
        cur.close()
        conn.close()
        client_mongo.close()
        stop_event.set()  # Detiene la animación de carga
        loader_thread.join()  # Espera a que el hilo de la animación termine

if __name__ == "__main__":
    main()

