import pymongo
from pymongo import MongoClient
from datetime import datetime
import pandas as pd

MONGO_URI = 'mongodb://localhost:27017'

# Conexión a MongoDB
client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=100000, connectTimeoutMS=100000)
db = client["data_bsale"] # Modificar DB segun necesidad.

# Colecciones
count_documents_col = db["count_bsale_report"]
count_expenses_col = db["count_expenses"]
sales_report_col = db["sales_report"]

gasto_produccion = db["gasto_produccion"]


# Total de documentos por sucursal, año y mes
doc_pipeline = [
    {
        "$addFields": {
            "local": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$local", "Tome"]}, "then": "Tomé"},
                        {"case": {"$eq": ["$local", "Quilpué"]}, "then": "Quilpue"},
                        {"case": {"$eq": ["$local", "San Ramon"]}, "then": "San Ramón"},
                        {"case": {"$eq": ["$local", "Plaza Maipu"]}, "then": "Plaza Maipú"},
                        {"case": {"$eq": ["$local", "LatinPizza Plaza Maipu"]}, "then": "Patio Maipu"},
                        {"case": {"$eq": ["$local", "Latin Pizza Buin"]}, "then": "Patio Buin"},
                        {"case": {"$eq": ["$local", "Maipu Vespucio"]}, "then": "Maipú Vespucio"},
                        {"case": {"$eq": ["$local", "Maipú 4 Poniente 2"]}, "then": "4 Poniente 2"},
                        {"case": {"$eq": ["$local", "Maipu 4 Poniente"]}, "then": "4 Poniente"},
                        {"case": {"$eq": ["$local", "Los Angeles"]}, "then": "Los Ángeles"},
                        {"case": {"$eq": ["$local", "Latin Pizza 4 Poniente"]}, "then": "4 Poniente 2"}
                    ],
                    "default": "$local"
                }
            }
        }
    },
    {
        "$group": {
            "_id": {"local": "$local", "year": "$year", "month": "$month"},
            "total_documents": {"$sum": "$subtotal_bruto"},
            "total_docs_count": {"$sum": "$total_documentos"}
        }
    }
]

doc_results = list(count_documents_col.aggregate(doc_pipeline))
#print("Document Results:", doc_results)



# Total de gastos por sucursal, año y mes
expense_pipeline = [
    {
        "$group": {
            "_id": {"sucursal": "$sucursal", "year": "$year", "month": "$month"},
            "total_expenses": {"$sum": "$total_monto"},
            "total_expense_count": {"$sum": "$cantidad_gastos"}
        }
    }
]

expense_results = list(count_expenses_col.aggregate(expense_pipeline))
#print("Expense Results:", expense_results)



# Ventas totales por sucursal, año y mes
sales_pipeline = [
    {
        "$group": {
            "_id": {
                "store_name": "$store_name", 
                "year": {"$year": "$date"}, 
                "month": {"$month": "$date"}
            },
            "total_sales": {"$sum": "$sub_total"}
        }
    },
    {
        "$project": {
            "_id": 0,  # Eliminamos _id porque ya no lo necesitamos
            "store_name": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$_id.store_name", "Con Con (Test)"]}, "then": "Con Con"},
                        {"case": {"$eq": ["$_id.store_name", "Curacavi (Test)"]}, "then": "Curacavi"},
                        {"case": {"$eq": ["$_id.store_name", "La Cisterna (Test)"]}, "then": "La Cisterna"},
                        {"case": {"$eq": ["$_id.store_name", "Quilpúe"]}, "then": "Quilpue"},
                        {"case": {"$eq": ["$_id.store_name", "Renca Poniente (Test)"]}, "then": "Renca Poniente"},
                        {"case": {"$eq": ["$_id.store_name", "Maipú 4 Poniente"]}, "then": "4 Poniente"},
                        {"case": {"$eq": ["$_id.store_name", "Kami Maipú 4 Poniente 2"]}, "then": "4 Poniente 2"},
                        {"case": {"$eq": ["$_id.store_name", "Kamisushi Patio Buin"]}, "then": "Patio Buin"},
                        {"case": {"$eq": ["$_id.store_name", "Kamisushi Patio Centro"]}, "then": "Patio Centro"},
                        {"case": {"$eq": ["$_id.store_name", "Kamisushi Patio Egaña"]}, "then": "Patio Egaña"},
                        {"case": {"$eq": ["$_id.store_name", "Kamisushi Patio Maipú"]}, "then": "Patio Maipu"},
                        {"case": {"$eq": ["$_id.store_name", "Latin Patio Buin"]}, "then": "Patio Buin"},
                        {"case": {"$eq": ["$_id.store_name", "Latin Patio Centro"]}, "then": "Patio Centro"},
                        {"case": {"$eq": ["$_id.store_name", "Latin Patio Egaña"]}, "then": "Patio Egaña"},
                        {"case": {"$eq": ["$_id.store_name", "Latin Patio Maipú"]}, "then": "Patio Maipu"},
                        {"case": {"$eq": ["$_id.store_name", "Latin Patio Maipú"]}, "then": "Patio Maipu"}
                    ],
                    "default": "$_id.store_name"
                }
            },
            "year": "$_id.year",
            "month": "$_id.month",
            "total_sales": 1
        }
    }
]

sales_results = list(sales_report_col.aggregate(sales_pipeline))
#print("Sales Results:", sales_results)


# Unir todos los resultados en un DataFrame
data = []


# Crear un diccionario para combinar datos por sucursal, año y mes
combined_data = {}


for doc in doc_results:
    key = (doc["_id"]["local"], doc["_id"]["year"], doc["_id"]["month"])

    if key not in combined_data:
        combined_data[key] = {
            "local": doc["_id"]["local"],
            "year": doc["_id"]["year"],
            "month": doc["_id"]["month"],
            "total_documents": doc["total_documents"],
            "total_docs_count": doc["total_docs_count"],
            "total_expenses": 0,
            "total_expense_count": 0,
            "total_sales": 0
        }

    else:
        combined_data[key]["total_documents"] += doc["total_documents"]
        combined_data[key]["total_docs_count"] += doc["total_docs_count"]



for exp in expense_results:
    key = (exp["_id"]["sucursal"], exp["_id"]["year"], exp["_id"]["month"])

    if key not in combined_data:
        combined_data[key] = {
            "local": exp["_id"]["sucursal"],
            "year": exp["_id"]["year"],
            "month": exp["_id"]["month"],
            "total_documents": 0,
            "total_docs_count": 0,
            "total_expenses": exp["total_expenses"],
            "total_expense_count": exp["total_expense_count"],
            "total_sales": 0
        }

    else:
        combined_data[key]["total_expenses"] += exp["total_expenses"]
        combined_data[key]["total_expense_count"] += exp["total_expense_count"]


for sale in sales_results:
    key = (sale["store_name"], sale["year"], sale["month"])

    if key not in combined_data:
        combined_data[key] = {
            "local": sale["store_name"],
            "year": sale["year"],
            "month": sale["month"],
            "total_documents": 0,
            "total_docs_count": 0,
            "total_expenses": 0,
            "total_expense_count": 0,
            "total_sales": sale["total_sales"]
        }

    else:
        combined_data[key]["total_sales"] += sale["total_sales"]



# Convertir el diccionario a DataFrame
for key, value in combined_data.items():
    value["gasto_produccion"] = (value["total_expenses"] + value["total_documents"]) / value["total_sales"] if value["total_sales"] > 0 else 0
    data.append(value)


df = pd.DataFrame(data)
#print("Combined Data:", df)

# Guardar el DataFrame en formato CSV
#csv_file_path = r"C:\Users\siste\OneDrive\Escritorio\Reportes Python\Mozart\reporte_gasto_produccion.csv"
#df.to_csv(csv_file_path, index=False)

# Guardar el DataFrame en formato Excel
excel_file_path = r"reporte_gasto_produccion.xlsx"
df.to_excel(excel_file_path, index=False)

#print(f"Reporte guardado en formato CSV: {csv_file_path}")
print(f"Reporte guardado en formato Excel: {excel_file_path}")

# Insertar o actualizar los datos en la colección de MongoDB (sin duplicar)
for record in df.to_dict('records'):
    gasto_produccion.update_one(
        {"local": record["local"], "year": record["year"], "month": record["month"]},  # Filtro para verificar si ya existe
        {"$set": record},  # Si existe, se actualizan los valores
        upsert=True  # Si no existe, se inserta un nuevo documento
    )

print(f"Datos insertados correctamente en la colección de MongoDB 'gasto_produccion'.")
