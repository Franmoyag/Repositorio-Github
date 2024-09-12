import pymongo
import pandas as pd
from datetime import datetime, timezone
import calendar
import os

# Configuración de MongoDB
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'data_bsale' # Modificar Base de Datos.

# Conexión a MongoDB con tiempos de espera aumentados
client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=100000, connectTimeoutMS=100000)
db = client[DB_NAME]

# Obtener el mes y año actuales
now = datetime.now()
current_year = now.year
current_month = now.month

# Calcular la fecha de inicio y fin del mes actual
start_date = datetime(current_year, current_month, 1, 0, 0, 0, tzinfo=timezone.utc)
end_date = datetime(current_year, current_month, calendar.monthrange(current_year, current_month)[1], 23, 59, 59, tzinfo=timezone.utc)

# Convertir las fechas a formato Epoch
#start_date_epoch = int(start_date.timestamp())
#end_date_epoch = int(end_date.timestamp())

start_date_epoch = 1722470400 # Fecha Inicio Manual
end_date_epoch = 1725148799 # Fecha Termino Manual


# Pipeline de emulacion de reporte Bsale
pipeline = [
    #{
    #    "$match": {"address": {"$eq": "Alameda 430"}
    #               
    #               }
    #},
    {
        "$addFields": {
            "itemCount": {"$size": "$details.items"}
        }
    },
    {
        "$addFields": {
            "state": {"$convert": {"input": "$state", "to": "int", "onError": 0, "onNull": 0}}
        }
    },
    {
        "$match": {
            "document_type_info.name": {"$ne": "GUÍA DE DESPACHO ELECTRÓNICA T"},
            "state": {"$ne": 1},
            "emissionDate": {"$gte": start_date_epoch, "$lte": end_date_epoch}
        }
    },
    {
        "$unwind": {
            "path": "$details.items",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$addFields": {
            "document_type.id": {"$toInt": "$document_type.id"},
            "client.id": {"$toInt": "$client.id"},
            "user.id": {"$toInt": "$user.id"},
            "office.id": {"$toInt": "$office.id"},
            "details.items.variant.id": {"$toInt": "$details.items.variant.id"},
            "temp_unique_id": {"$concat": [{"$toString": "$_id"}, "_", {"$toString": {"$rand": {}}}]}
        }
    },
    {
        "$lookup": {
            "from": "clients",
            "localField": "client.id",
            "foreignField": "id",
            "as": "client_info"
        }
    },
    {
        "$unwind": {
            "path": "$client_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "document_types",
            "localField": "document_type.id",
            "foreignField": "id",
            "as": "document_type_info"
        }
    },
    {
        "$unwind": {
            "path": "$document_type_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "user.id",
            "foreignField": "id",
            "as": "user_info"
        }
    },
    {
        "$unwind": {
            "path": "$user_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "offices",
            "localField": "office.id",
            "foreignField": "id",
            "as": "office_info"
        }
    },
    {
        "$unwind": {
            "path": "$office_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "variants",
            "localField": "details.items.variant.id",
            "foreignField": "id",
            "as": "variant_info"
        }
    },
    {
        "$unwind": {
            "path": "$variant_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "variant_info.product.href",
            "foreignField": "href",
            "as": "product_info"
        }
    },
    {
        "$unwind": {
            "path": "$product_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$match": {
            "product_info.product_type.name": {"$in": ["DIRECTOS", "PAQUETERIA"]}
        }
    },
    {
        "$lookup": {
            "from": "price_lists",
            "let": {
                "variant_id": "$variant_info.id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$variant.id", "$$variant_id"]},
                                {"$eq": ["$condition", "LISTA VENTA"]}
                            ]
                        }
                    }
                }
            ],
            "as": "price_list_venta"
        }
    },
    {
        "$unwind": {
            "path": "$price_list_venta",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "price_lists",
            "let": {
                "variant_id": "$variant_info.id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$variant.id", "$$variant_id"]},
                                {"$eq": ["$condition", "LISTA COSTO"]}
                            ]
                        }
                    }
                }
            ],
            "as": "price_list_costo"
        }
    },
    {
        "$unwind": {
            "path": "$price_list_costo",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$addFields": {
            "_id": "$temp_unique_id",
            "Tipo Movimiento": {
                "$switch": {
                    "branches": [
                        {"case": {"$in": ["$document_type.id", [2, 9, 12]]}, "then": "Devolucion"},
                        {"case": {"$in": ["$document_type.id", [38, 1, 6, 15, 27]]}, "then": "Venta"}
                    ],
                    "default": ""
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$_id",
            "document": {"$first": "$$ROOT"}
        }
    },
    {
        "$replaceRoot": {
            "newRoot": "$document"
        }
    },
    {
        "$addFields": {
            "local": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$client.code", "16.376.062-2"]}, "then": "Otro"},
                        {"case": {"$eq": ["$client.code", "77.398.566-9"]}, "then": "Latin Pizza Conchali"},
                        {"case": {"$eq": ["$address", "Neptuno 823"]}, "then": "Lo Prado"},
                        {"case": {"$eq": ["$address", "AMERICO VESPUCIO 456   AMRICA DEL SUR"]}, "then": "Maipu Vespucio"},
                        {"case": {"$eq": ["$address", "AV LO CRUZAT 0460"]}, "then": "Quilicura"},
                        {"case": {"$eq": ["$address", "ESPERANZA 789"]}, "then": "Santiago Centro"},
                        {"case": {"$eq": ["$address", "MANUTARA 8992"]}, "then": "La Florida"},
                        {"case": {"$eq": ["$address", "RIO DE JANEIRO 428"]}, "then": "Independencia"},
                        {"case": {"$eq": ["$address", "AVENIDA PORTALES 330"]}, "then": "Plaza Maipu"},
                        {"case": {"$eq": ["$address", "DOMINGO SANTA MARIA 3259"]}, "then": "Renca"},
                        {"case": {"$eq": ["$address", "BULNES 608  A"]}, "then": "San Bernardo"},
                        {"case": {"$eq": ["$address", "APOQUINDO 5232"]}, "then": "Las Condes"},
                        {"case": {"$eq": ["$address", "Gabriela Oriente 2220"]}, "then": "Puente Alto"},
                        {"case": {"$eq": ["$address", "AV CENTRAL 6578"]}, "then": "Lo Espejo"},
                        {"case": {"$eq": ["$address", "AVENIDA MANUEL RODRIGUEZ 1915"]}, "then": "Patio Centro"},
                        {"case": {"$eq": ["$address", "DIAGONAL ORIENTE 5353"]}, "then": "Patio Egaña"},
                        {"case": {"$eq": ["$address", "Av. Padre Hurtado Nº 13280"]}, "then": "El Bosque"},
                        {"case": {"$eq": ["$address", "SANTA ROSA 7917"]}, "then": "San Ramon"},
                        {"case": {"$eq": ["$address", "JOSE MIGUEL CARRERA 8637"]}, "then": "La Cisterna"},
                        {"case": {"$eq": ["$address", "Av. Macul 4591-4593"]}, "then": "Macul"},
                        {"case": {"$eq": ["$address", "Av. Club Hípico 5424"]}, "then": "Pedro Aguirre Cerda"},
                        {"case": {"$eq": ["$address", "ALMIRANTE BLANCO ENCALADA Nro:2445"]}, "then": "Estacion Central"},
                        {"case": {"$eq": ["$address", "Gran Avenida 4399"]}, "then": "San Miguel"},
                        {"case": {"$eq": ["$address", "AV BRASIL 7844"]}, "then": "Renca Poniente"},
                        {"case": {"$eq": ["$address", "BAJOS DE MATTE 0510 2"]}, "then": "Buin"},
                        {"case": {"$eq": ["$address", "INDEPENDENCIA 5741"]}, "then": "Conchali"},
                        {"case": {"$eq": ["$address", "Departamental Nro 36, Dpto AyB"]}, "then": "San Joaquin"},
                        {"case": {"$eq": ["$address", "CUATRO PONIENTE 01121 dpto 2"]}, "then": "Maipu 4 Poniente"},
                        {"case": {"$eq": ["$address", "Calle J Arrieta 6599, Los Guindos"]}, "then": "Peñalolen"},
                        {"case": {"$eq": ["$address", "Las Rejas Sur Nro 1392"]}, "then": "Las Rejas"},
                        {"case": {"$eq": ["$address", "AV PEDRO FONTOVA 7810, DPTO 4"]}, "then": "Huechuraba"},
                        {"case": {"$eq": ["$address", "CALLE 1 11 CANTAROS DE AGUA"]}, "then": "Peñaflor"},
                        {"case": {"$eq": ["$address", "Carretera Gral. San Mar 359 Dpto 6"]}, "then": "Colina"},
                        {"case": {"$eq": ["$address", "LO ERRAZURIZ 2091"]}, "then": "Cerrillos"},
                        {"case": {"$eq": ["$address", "MARCOLETA Nro 558"]}, "then": "Marcoleta"},
                        {"case": {"$eq": ["$address", "LUIS MATTE LARRAIN Nro 1360"]}, "then": "Puente Alto 2"},
                        {"case": {"$eq": ["$address", "PLAZA EL SALTO 090"]}, "then": "El Salto"},
                        {"case": {"$eq": ["$address", "BERNANRDO OHIHHING 629"]}, "then": "Talagante"},
                        {"case": {"$eq": ["$address", "Diagonal rancagua 918"]}, "then": "Providencia"},
                        {"case": {"$eq": ["$address", "Cardenal Raúl Silva Henríquez 10.002 b"]},"then": "La Granja"},
                        {"case": {"$eq": ["$address", "Carretera general San Martín 042 local 1"]}, "then": "Colina 2"},
                        {"case": {"$eq": ["$address", "Presbitero Moraga 307, Local B"]}, "then": "Curacavi"},
                        {"case": {"$eq": ["$address", "EL MANZANO 391 "]}, "then": "Padre Hurtado"},
                        {"case": {"$eq": ["$address", "5 norte 315"]}, "then": "Viña del mar"},
                        {"case": {"$eq": ["$address", "Alameda 430"]}, "then": "Rancagua"},
                        {"case": {"$eq": ["$address", "Manuel montt 689 local 2"]}, "then": "Villa Alemana"},
                        {"case": {"$eq": ["$address", "Avenida Concon Reñaca 41, Local 2"]}, "then": "Con Con"},
                        {"case": {"$eq": ["$address", "Av Condell #201"]}, "then": "Quillota"},
                        {"case": {"$eq": ["$address", "M RODRIGUEZ 1202"]}, "then": "Concepción"},
                        {"case": {"$eq": ["$address", "TEGUALDA NORTE 38 LC 6 6 CONCEPCION"]}, "then": "Concepción 2 Collao"},
                        {"case": {"$eq": ["$address", "LOS AROMOS 1477 SAN PEDRO DE LA PAZ"]}, "then": "San Pedro de la Paz"},
                        {"case": {"$eq": ["$address", "Avenida Latorre 454 LOCAL 5"]}, "then": "Tome"},
                        {"case": {"$eq": ["$address", "MANUEL MONTT 02500 2 CORONEL"]}, "then": "Coronel"},
                        {"case": {"$eq": ["$address", "Manuel Rodríguez 1898 local 4, Chiguayante"]}, "then": "Chiguayante"},
                        {"case": {"$eq": ["$address", "COLON 1474 1478 1478 1 TALCAHUANO"]}, "then": "Talcahuano"},
                        {"case": {"$eq": ["$address", "ARTURO PRAT 449 - 453 A CALERA"]}, "then": "La Calera"},
                        {"case": {"$eq": ["$address", "RAMON FREIRE 899 1"]}, "then": "Quilpue"},
                        {"case": {"$eq": ["$address", "avenida alemania 690 Los angeles"]}, "then": "Los Angeles"},
                        {"case": {"$eq": ["$address", "Coronel alejandro sepulveda 1789"]}, "then": "Peñalolen 2"},
                        {"case": {"$eq": ["$address", "MANUEL MONTT 0560 CORONEL"]}, "then": "Coronel 2"},
                        {"case": {"$eq": ["$address", "Canal la punta 8770"]}, "then": "Kamver"},
                        {"case": {"$eq": ["$address", "SANTA ROSA Nro 7917"]}, "then": "Latin pizza san ramon"},
                        {"case": {"$eq": ["$address", "Carretera General San Martín 359 Local 7"]}, "then": "Latin Pizza Colina"},
                        {"case": {"$eq": ["$address", "Av. Central Cardenal Raúl Silva Henfiques 7456 ex 6756 Local B"]}, "then": "Latin Pizza Lo Espejo"},
                        {"case": {"$eq": ["$address", " El Manzano 391 Local 3"]}, "then": "Latin Pizza Padre Hurtado"},
                        {"case": {"$eq": ["$address", "QUENAC 6110"]}, "then": "Latin Pizza Las Rejas"},
                        {"case": {"$eq": ["$address", "AVENIDA PADRE HURTADO 13180"]}, "then": "Latin Pizza El Bosque"},
                        {"case": {"$eq": ["$address", "Joaquín Edward Bello 10488 A"]}, "then": "Latin Pizza La Granja"},
                        {"case": {"$eq": ["$address", "Av. Pdte Kennedy 817"]}, "then": "Latin Pizza Buin"},
                        {"case": {"$eq": ["$address", "Presbitero Moraga 307 C"]}, "then": "Latin Pizza Curacavi"},
                        {"case": {"$eq": ["$address", "Lo Errazuriz 2091 A"]}, "then": "Latin Pizza Cerrillos"},
                        {"case": {"$eq": ["$address", "AV.VIC.MACKENNA 1545 Bloque:11 Depto.:K"]}, "then": "Latin Pizza Peñaflor"},
                        {"case": {"$eq": ["$address", "Av 4 Poniente 0383-B"]}, "then": "Latin Pizza 4 Poniente"},
                        {"case": {"$eq": ["$address", "Alberto LLona 1432"]}, "then": "LatinPizza Plaza Maipu"},
                        {"case": {"$eq": ["$address", "Libertador Bernardo O'higgins 446 #2"]}, "then": "Latin Pizza Talagante"},
                        {"case": {"$eq": ["$address", "Av Carrascal 4652"]}, "then": "Latin Pizza Quinta Normal"},
                        {"case": {"$eq": ["$address", "AVENIDA EL SALTO 3260 LOCAL 1"]}, "then": "Latin Pizza El Salto / Recoleta"},
                        {"case": {"$eq": ["$address", "DOMINGO SANTA MARIA 3195"]}, "then": "Latin Pizza Renca"},
                        {"case": {"$eq": ["$address", "CONCHA Y TORO 610 - LOCAL 3"]}, "then": "Puente Alto 3"},
                        {"case": {"$eq": ["$address", "SN PABLO 6004 LAUTARO LO PRADO"]}, "then": "San Pablo"},
                        {"case": {"$eq": ["$address", "Pje. Cinco 2603"]}, "then": "Hualpén"},
                        {"case": {"$eq": ["$address", "Carretera general San Martín 042  local 1"]}, "then": "Colina 2"}
                    ],
                    "default": "Sin Sucursal"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "Tipo Movimiento": "$Tipo Movimiento",
            "Sucursal": "$office.name",
            "Vendedor": {"$concat": ["$user.firstName", " ", "$user.lastName"]},
            "Cliente": "$client.company",
            "Rut Cliente": "$client.code",
            "Tipo Documento": "$document_type_info.name",
            "N° Documento": "$number",
            "Local": "$local",
            "Tracking Number": "$trackingNumber",
            "Fecha Documento": {"$dateToString": {"format": "%d/%m/%Y", "date": {"$toDate": {"$multiply": ["$emissionDate", 1000]}}}},
            "Fecha y Hora de Venta": {"$let": {
                "vars": {
                    "adjustedDate": {"$dateAdd": {"startDate": {"$toDate": {"$multiply": ["$generationDate", 1000]}}, "unit": "hour", "amount": -4}}
                },
                "in": {"$concat": [{"$dateToString": {"format": "%d/%m/%Y", "date": "$$adjustedDate"}}, " ",
                                    {"$cond": {"if": {"$lte": [{"$hour": {"date": "$$adjustedDate"}}, 12]},
                                                "then": {"$dateToString": {"format": "%H:%M AM", "date": "$$adjustedDate"}},
                                                "else": {"$dateToString": {"format": "%H:%M PM", "date": "$$adjustedDate"}}}}]}
            }},
            "Lista de Precio": "$client_info.price_list.name",
            "Tipo de Producto / Servicio": "$product_info.product_type.name",
            "SKU": "$variant_info.code",
            "Producto / Servicio": "$product_info.name",
            "Variante": "$variant_info.description",
            "Precio Neto Unitario": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                    "then": { "$multiply": ["$details.items.netUnitValue", -1] }, # Se cambia -1
                                    "else": "$details.items.netUnitValue"}},0]},
            "Cantidad": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                        "then": { "$multiply": ["$details.items.quantity", -1] }, # Se cambia -1
                                        "else": "$details.items.quantity"}},1]},
            "Subtotal Impuestos": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                    "then": { "$multiply": ["$details.items.taxAmount", -1] }, # Se cambia -1
                                    "else": "$details.items.taxAmount"}},0]},
            "Subtotal Bruto": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                    "then": { "$multiply": ["$details.items.totalAmount", -1] }, # Se cambia -1
                                    "else": "$details.items.totalAmount"}},0]},
            "Costo Neto Unitario": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                        "then": { "$multiply": ["$price_list_costo.variantValue", -1] }, # Se cambia -1
                                        "else": "$price_list_costo.variantValue"}},1]},
            "Margen": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                        "then": { "$multiply": [{"$round": [{"$multiply": [{"$subtract": ["$price_list_venta.variantValue", "$price_list_costo.variantValue"]}, "$details.items.quantity"]}, 2]}, -1] },#Se cambia -1
                                        "else": { "$round": [{"$multiply": [{"$subtract": ["$price_list_venta.variantValue", "$price_list_costo.variantValue"]}, "$details.items.quantity"]}, 2] }}},1]},
            "Costo Total Neto": {"$round": [{"$cond": {"if": { "$eq": ["$Tipo Movimiento", "Devolucion"] },
                                        "then": { "$multiply": [{"$round": [{"$multiply": ["$details.items.quantity", "$price_list_costo.variantValue"]}, 2]}, -1] }, # Se cambia -1
                                        "else": { "$round": [{"$multiply": ["$details.items.quantity", "$price_list_costo.variantValue"]}, 2] }}},1]},
            "% Margen": {"$round": [{"$cond": {
                "if": {"$gt": [{"$multiply": ["$details.items.quantity", "$price_list_costo.variantValue"]}, 1]},
                "then": {"$multiply": [{"$divide": [
                    {"$multiply": [{"$subtract": ["$price_list_venta.variantValue","$price_list_costo.variantValue"]}, "$details.items.quantity"]}, 
                    {"$multiply": ["$details.items.quantity", "$price_list_costo.variantValue"]}]}, 100]},
                "else": 0
            }}, 1]},
            "Direccion Cliente": "$address",
            "Comuna Cliente": "$municipality",
            "Ciudad Cliente": "$city"
        }
    },
    {
        "$match": {
            "Tipo Documento": {"$ne": "GUÍA DE DESPACHO ELECTRÓNICA T"},
            "state": {"$ne": 1}
        }
    }
]



# Ejecutar la consulta
collection = db['documents']


# Utilizar cursores para procesar datos por partes
cursor = collection.aggregate(pipeline, allowDiskUse=True)

# Convertir los resultados en una lista
results = list(cursor)


# Verificar si hay resultados antes de crear el DataFrame
if results:
    try:
        # Crear DataFrame directamente desde los resultados
        df = pd.DataFrame(results)

        print("Datos obtenidos correctamente:")
        print(df.head())  # Muestra los primeros 5 resultados para depuración

        # Verificar cuántas filas hay en el DataFrame
        print(f"Total de documentos obtenidos: {len(df)}")

        # Almacenar los resultados en MongoDB (Nombre de Colección)
        output_collection = db['bsale_report']

        # Iterar sobre cada fila del DataFrame para insertar/actualizar en MongoDB
        for _, row in df.iterrows():
            document_dict = row.to_dict()

            # Definir un filtro único para evitar duplicados. Asegúrate de que el filtro sea lo suficientemente específico.
            unique_filter = {
                "N° Documento": document_dict.get("N° Documento"),
                "SKU": document_dict.get("SKU"),
                "Producto / Servicio": document_dict.get("Producto / Servicio")
            }

            # Usar upsert=True para insertar si no existe o actualizar si ya existe
            output_collection.update_one(unique_filter, {"$set": document_dict}, upsert=True)

        # Guardar el archivo CSV final (Modificar Directorio segun necesidad.)
        file_name = now.strftime("%B_%Y").lower()  # Formato "mes_año"
        output_file = f'C:/Users/siste/OneDrive/Escritorio/Reportes Python/Mozart/reporte_{file_name}.csv'
        df.to_csv(output_file, index=False)

        print(f'Reporte guardado en {output_file}')
        print("Datos almacenados en la colección sin duplicados.")

    except Exception as e:
        print(f"Ocurrió un error durante la conversión a DataFrame o el guardado en la colección: {e}")
else:
    print("No se encontraron resultados para procesar.")
