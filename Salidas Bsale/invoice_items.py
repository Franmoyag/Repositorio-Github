import json
import pandas as pd


# Mapa de address a tienda
address_to_store = {
    "AV PEDRO FONTOVA 7810, DPTO 4": "Lo Prado",
    "AMERICO VESPUCIO 456   AMRICA DEL SUR": "Maipu Vespucio",
    "AV LO CRUZAT 0460": "Quilicura",
    "ESPERANZA 789": "Santiago Centro",
    "MANUTARA 8992": "La Florida",
    "RIO DE JANEIRO 428": "Independencia",
    "AVENIDA PORTALES 330": "Plaza Maipu",
    "DOMINGO SANTA MARIA 3259": "Renca",
    "BULNES 608  A": "San Bernardo",
    "APOQUINDO 5232": "Las Condes",
    "Gabriela Oriente 2220": "Puente Alto",
    "AV CENTRAL 6578": "Lo Espejo",
    "AVENIDA MANUEL RODRIGUEZ 1915": "Patio Centro",
    "DIAGONAL ORIENTE 5353": "Patio Egaña",
    "Av. Padre Hurtado Nº 13280": "El Bosque",
    "SANTA ROSA 7917": "San Ramon",
    "JOSE MIGUEL CARRERA 8637": "La Cisterna",
    "Av. Macul 4591-4593": "Macul",
    "Av. Club Hípico 5424": "Pedro Aguirre Cerda",
    "ALMIRANTE BLANCO ENCALADA Nro:2445": "Estacion Central",
    "Gran Avenida 4399": "San Miguel",
    "AV BRASIL 7844": "Renca Poniente",
    "BAJOS DE MATTE 0510 2": "Buin",
    "INDEPENDENCIA 5741": "Conchali",
    "Departamental Nro 36, Dpto AyB": "San Joaquin",
    "CUATRO PONIENTE 01121 dpto 2": "Maipu 4 Poniente",
    "Calle J Arrieta 6599, Los Guindos": "Peñalolen",
    "Las Rejas Sur Nro 1392": "Las Rejas",
    "AV PEDRO FONTOVA 7810, DPTO 4": "Huechuraba",
    "CALLE 1 11 CANTAROS DE AGUA": "Peñaflor",
    "Carretera Gral. San Mar 359 Dpto 6": "Colina",
    "LO ERRAZURIZ 2091": "Cerrillos",
    "MARCOLETA Nro 558": "Marcoleta",
    "LUIS MATTE LARRAIN Nro 1360": "Puente Alto 2",
    "PLAZA EL SALTO 090": "El Salto",
    "BERNANRDO OHIHHING 629": "Talagante",
    "Diagonal rancagua 918": "Providencia",
    "Cardenal Raúl Silva Henríquez 10.002 b": "La Granja",
    "Carretera general San Martín 042  local 1": "Colina 2",
    "Presbitero Moraga 307, Local B": "Curacavi",
    "EL MANZANO 391 ": "Padre Hurtado",
    "5 norte 315": "Viña del mar",
    "Alameda 430": "Rancagua",
    "Manuel montt 689 local 2": "Villa Alemana",
    "Avenida Concon Reñaca 41, Local 2": "Con Con",
    "Av Condell #201": "Quillota",
    "M RODRIGUEZ 1202": "Concepcion",
    "TEGUALDA NORTE 38 LC 6 6 CONCEPCION": "Concepcion 2 Collao",
    "LOS AROMOS 1477 SAN PEDRO DE LA PAZ": "San Pedro de la Paz",
    "Avenida Latorre 454 LOCAL 5": "Tome",
    "MANUEL MONTT 02500 2 CORONEL": "Coronel",
    "Manuel Rodríguez 1898 local 4, Chiguayante": "Chiguayante",
    "COLON 1474 1478 1478 1 TALCAHUANO": "Talcahuano",
    "ARTURO PRAT 449 - 453 A CALERA": "La Calera",
    "RAMON FREIRE 899 1": "Quilpue",
    "avenida alemania 690 Los angeles": "Los Angeles",
    "Coronel alejandro sepulveda 1789": "Peñalolen 2",
    "MANUEL MONTT 0560 CORONEL": "Coronel 2"
}

# Ruta de Archivo y su codificacion.
with open('C:/Users/siste/OneDrive/Escritorio/Facturas Bsale/Ventas/api_data.xml_details.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Lista para almacenar los datos que se convertirán en CSV
csv_data = []
unique_addresses = set()  # Conjunto para almacenar direcciones únicas

# Iterar sobre cada factura en el JSON
for invoice in data:
    document_type = invoice['factura_info']['document_type']['id']
    typeDte_description = invoice.get('typeDte_description', '')
    
    # Verificar si client_info no es None y obtener address
    client_info = invoice.get('client_info')
    address = client_info['address'] if client_info and 'address' in client_info else 'No Address'
    unique_addresses.add(address)  # Agregar la dirección al conjunto de direcciones únicas
    tienda = address_to_store.get(address, "No Definido")
    
    for item in invoice['items']:
        csv_data.append({
            "invoice_number": invoice["invoice_number"],
            "document_type": document_type,
            "typeDte_description": typeDte_description,
            "client_name": invoice["client_name"],
            "address": address,
            "tienda": tienda,
            "date": invoice["date"],
            "description": item["description"],
            "product_type_name": item["product_type_name"],
            "quantity": item["quantity"].replace('.', ','),  # Reemplazar puntos con comas
            "price": str(item["price"]).replace('.', ','),  # Reemplazar puntos con comas
            "total": str(item["total"]).replace('.', ',')  # Reemplazar puntos con comas
        })

# Imprimir las direcciones únicas
print("Direcciones únicas encontradas en los datos:")
for addr in unique_addresses:
    print(addr)

# Convertir la lista de diccionarios en un DataFrame de pandas
df = pd.DataFrame(csv_data)

# Guardar el DataFrame como un archivo CSV
df.to_csv('C:/Users/siste/OneDrive/Escritorio/Facturas Bsale/Ventas/invoice_items.csv', index=False)

# Mostrar el DataFrame
print(df)
