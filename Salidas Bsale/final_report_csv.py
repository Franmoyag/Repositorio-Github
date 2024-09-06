from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient('localhost', 27017)
db = client['api_data']

# Pipeline de agregación
pipeline = [
    {
        "$lookup": {
            "from": "xml_details",
            "localField": "number",
            "foreignField": "invoice_number",
            "as": "xml_details"
        }
    },
    {
        "$unwind": "$xml_details"
    },
    {
        "$lookup": {
            "from": "clients",
            "localField": "client.id",
            "foreignField": "id",
            "as": "client_details"
        }
    },
    {
        "$unwind": "$client_details"
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "xml_details.items.productCode",
            "foreignField": "id",
            "as": "product_details"
        }
    },
    {
        "$unwind": "$product_details"
    },
    {
        "$lookup": {
            "from": "type_products",
            "localField": "product_details.product_type.id",
            "foreignField": "id",
            "as": "type_product_details"
        }
    },
    {
        "$unwind": "$type_product_details"
    },
    {
        "$project": {
            "_id": 0,
            "Tipo Movimiento": "$tipoMovimiento",
            "Sucursal": "$sucursal",
            "Vendedor": "$vendedor",
            "Cliente": "$client_details.company",
            "Rut Cliente": "$client_details.code",
            "Tipo de Documento": "$xml_details.typeDte_description",
            "Numero Documento": "$number",
            "Fecha de Documento": "$emissionDate",
            "Fecha y Hora de Venta": "$createdAt",
            "Lista de Precio": "$client_details.price_list.id",
            "Tipo de Producto / Servicio": "$type_product_details.name",
            "SKU": "$xml_details.items.productCode",
            "Producto / Servicio": "$product_details.name",
            "Variante": "$xml_details.items.variant",
            "Precio Neto Unitario": "$xml_details.items.price",
            "Cantidad": "$xml_details.items.quantity",
            "Subtotal Impuestos": "$taxAmount",
            "Subtotal Bruto": "$totalAmount",
            "Costo Neto Unitario": "$cost",
            "Margen": "$margin",
            "Costo Total Neto": "$totalCost",
            "% Margen": "$marginPercentage",
            "Dirección Cliente": "$client_details.address",
            "Comuna Cliente": "$client_details.municipality",
            "Ciudad Cliente": "$client_details.city",
            "Email Cliente": "$client_details.email"
        }
    }
]

# Ejecutar la agregación y guardar el resultado en una nueva colección
result = list(db.facturas_2024.aggregate(pipeline))
db.final_report.insert_many(result)

print(f"Reporte generado y guardado en la colección 'final_report'.")
