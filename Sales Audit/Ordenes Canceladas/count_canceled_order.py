from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

# Conexion Mongo
client = MongoClient('mongodb://localhost:27017')
db = client ['data_bsale']
coleccion_origen = db ['orders_canceled']
coleccion_destino = db ['canceled_count']


sucursal_mapping = {
    1: "Sucursal de prueba",
    2: "Lo Prado",
    3: "Maipú Vespucio",
    4: "Quilicura",
    5: "Santiago Centro",
    6: "Ñuñoa",
    7: "La Florida",
    8: "Independencia",
    9: "Plaza Maipú",
    10: "Renca",
    11: "Restaurant Colombiano",
    12: "San Bernardo",
    13: "Las Condes",
    14: "Puente Alto",
    15: "Lo Espejo",
    16: "NT",
    17: "Patio Egaña",
    18: "El Bosque",
    19: "San Ramón",
    20: "La Cisterna",
    21: "Latin Pizza San Ramon",
    22: "Macul",
    23: "Pedro Aguirre Cerda",
    24: "Estacion Central",
    25: "San Miguel",
    26: "Renca Poniente",
    27: "Buin",
    28: "Conchali",
    29: "San Joaquin",
    30: "Maipú 4 Poniente",
    31: "Peñalolen",
    32: "Las Rejas",
    33: "Huechuraba",
    34: "Peñaflor",
    35: "Colina",
    36: "Cerrillos",
    37: "Marcoleta",
    38: "Puente Alto 2",
    39: "El Salto",
    40: "Talagante",
    41: "Providencia",
    42: "La Granja",
    43: "Colina 2",
    44: "Curacavi",
    45: "Padre Hurtado",
    46: "Viña del Mar",
    47: "Rancagua",
    48: "Villa Alemana",
    49: "Con Con",
    50: "Quillota",
    51: "Latin Pizza Colina",
    52: "Latin Pizza Padre Hurtado",
    53: "Latin Pizza Lo Espejo",
    54: "Latin Pizza Conchali",
    55: "Latin Pizza Las Rejas",
    56: "Latin Pizza Plaza Egaña",
    57: "Latin Pizza Cerrillos",
    58: "Latin Pizza La Granja",
    59: "Latin Pizza Curacavi",
    60: "Latin Pizza Buin",
    61: "Latin Pizza El Bosque",
    62: "Latin Pizza Peñaflor",
    63: "Latin Pizza 4 Poniente",
    64: "Latin Pizza Quinta Normal",
    65: "Latin Pizza Plaza Maipu",
    66: "Latin Pizza El Salto / Recoleta",
    67: "Latin Pizza Santiago",
    68: "Latin Pizza Talagante",
    69: "Latin Pizza Renca",
    70: "Kami Cerrito",
    71: "Kami El Cerro",
    72: "Kami Las Piedras",
    73: "Kami Las Tejas",
    80: "Concepción",
    81: "Concepción 2 Collao",
    82: "San Pedro de la Paz",
    83: "Tomé",
    84: "Coronel",
    85: "Chiguayante",
    86: "Talcahuano",
    87: "Hualpén",
    88: "Lota",
    89: "La Calera",
    90: "Quilpúe",
    91: "Los Ángeles",
    92: "Peñalolen 2",
    93: "Coronel 2",
    94: "San Pablo",
    95: "Con Con (Test)",
    96: "Puente Alto 3",
    97: "La Pintana",
    98: "Machalí",
    99: "Renca Poniente (Test)",
    100: "La Cisterna (Test)",
    101: "Curacavi (Test)",
    1000: "Kami Pruebas",
    2000: "TEST_HB"
}


def procesar_y_actualizar_datos():
    # Obtener todas las jornadas únicas
    jornadas_unicas = coleccion_origen.distinct('jornada')
    
    for jornada in jornadas_unicas:
        # Obtener todos los índices únicos para la jornada actual
        indices_unicos = coleccion_origen.distinct('index', {'jornada': jornada})
        
        for index in indices_unicos:
            # Filtrar documentos por jornada e índice actual
            documentos = coleccion_origen.find({'jornada': jornada, 'index': index})
            
            acumulador = {}
            
            for doc in documentos:
                sucursal_id = doc.get('sucursal')
                sucursal_name = sucursal_mapping.get(sucursal_id, 'Sucursal Desconocida')
                fecha_obj = doc.get('fecha')

                if fecha_obj:
                    fecha = fecha_obj.strftime("%d-%m-%Y")
                else:
                    fecha = None

                # Clave para agrupar (sucursal y fecha)
                clave = (sucursal_name, fecha)

                # Sumar los productos para cada grupo
                productos = doc.get('productos', [])
                monto_total = sum([producto.get('price', 0) * producto.get('quantity', 1) for producto in productos])

                if clave in acumulador:
                    acumulador[clave] += monto_total
                else:
                    acumulador[clave] = monto_total

            # Guardar datos agrupados en MongoDB
            for (sucursal_name, fecha), monto_total in acumulador.items():
                # Verificar si ya existe un registro con la misma Sucursal y Fecha en la colección de destino
                existe = coleccion_destino.find_one({
                    "Sucursal": sucursal_name,
                    "Date": fecha
                })

                if not existe:
                    # Insertar el registro si no existe
                    coleccion_destino.insert_one({
                        "Sucursal": sucursal_name,
                        "Date": fecha,
                        "Total Amount": monto_total
                    })
                else:
                    # Si ya existe, sumar el monto al existente
                    coleccion_destino.update_one(
                        {"Sucursal": sucursal_name, "Date": fecha},
                        {"$inc": {"Total Amount": monto_total}}
                    )

def guardar_excel():
    registros_actualizados = list(coleccion_destino.find({}, {"_id": 0, "Sucursal": 1, "Date": 1, "Total Amount": 1}))
    df_final = pd.DataFrame(registros_actualizados)

    mes_actual = datetime.now().strftime("%m")
    año_actual = datetime.now().strftime("%Y")
    nombre_archivo = f"canceled_report-KAMI-{mes_actual}-{año_actual}.xlsx"

    df_final.to_excel(nombre_archivo, index=False)

# Ejecutar funciones
procesar_y_actualizar_datos()
guardar_excel()