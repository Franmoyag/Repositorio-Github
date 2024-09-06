import requests
from datetime import datetime


ACCESS_TOKEN = 'e57d148002d91e58f152bece58d810e50bc84286'
BASE_URL = 'https://api.bsale.cl/v1/'

headers = {
    'access_token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

# Función para obtener todos los productos con variantes
def obtener_todos_los_productos():
    url = f"{BASE_URL}products.json?expand=variants"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error al obtener productos: {response.status_code} - {response.text}")
        return []

    data = response.json()
    if 'items' in data:
        return data['items']
    else:
        print("No se encontraron productos en la respuesta.")
        return []

# Función para obtener el stock de un producto específico
def obtener_stock_producto(product_id):
    url = f"{BASE_URL}stocks.json?product_id={product_id}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error al obtener stock para el producto {product_id}: {response.status_code} - {response.text}")
        return None

    try:
        data = response.json()
        total_stock = sum(item['quantity'] for item in data['items'])  # Sumar todos los valores de 'quantity'
        return total_stock
    except ValueError as e:
        print(f"Error al analizar la respuesta JSON para el producto {product_id}: {str(e)}")
        return None

# Función para obtener las recepciones de un producto específico por código
def obtener_recepciones_por_codigo(codigo_producto):
    url = f"{BASE_URL}stocks/receptions.json?expand=details"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error al obtener recepciones: {response.status_code} - {response.text}")
        return []

    try:
        data = response.json()
        recepciones = data.get('items', [])
        recepciones_filtradas = []

        for recepcion in recepciones:
            fecha_recepcion = datetime.utcfromtimestamp(recepcion['admissionDate']).strftime('%Y-%m-%d')
            detalles = recepcion['details']['items']
            
            for detalle in detalles:
                variant = detalle['variant']
                if variant['id'] == codigo_producto:
                    recepciones_filtradas.append({
                        'fecha': fecha_recepcion,
                        'cantidad': detalle['quantity']
                    })

        return recepciones_filtradas
    except ValueError as e:
        print(f"Error al analizar la respuesta JSON para recepciones: {str(e)}")
        return []

# Obtener el primer producto
productos = obtener_todos_los_productos()

if productos:
    # Seleccionar el primer producto
    primer_producto = productos[0]
    product_id = primer_producto['id']
    nombre_producto = primer_producto['name']
    
    # Verificar si existen variantes y si el campo 'items' dentro de 'variants' tiene elementos
    if 'variants' in primer_producto and 'items' in primer_producto['variants'] and primer_producto['variants']['items']:
        codigo_producto = primer_producto['variants']['items'][0].get('id', 'Sin Código')
    else:
        codigo_producto = 'Sin Código'

    # Obtener el stock del primer producto
    stock_actual = obtener_stock_producto(product_id)

    # Obtener las recepciones del producto por código
    recepciones = obtener_recepciones_por_codigo(codigo_producto)

    # Mostrar la información obtenida
    print(f"Producto ID: {product_id}")
    print(f"Nombre del Producto: {nombre_producto}")
    print(f"Código del Producto: {codigo_producto}")
    print(f"Stock Actual: {stock_actual}")
    print(f"Recepciones para el producto {codigo_producto}:")
    for recepcion in recepciones:
        print(f"  Fecha: {recepcion['fecha']}, Cantidad: {recepcion['cantidad']}")
else:
    print("No se encontraron productos.")
