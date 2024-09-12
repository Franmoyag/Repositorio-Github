from pymongo import MongoClient
from datetime import datetime
import pandas as pd

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["data_bsale"] # Modificar DB segun necesidad.

# Colecciones
count_documents_col = db["count_bsale_report"]
count_expenses_col = db["count_expenses"]
sales_report_col = db["sales_report"]



# Total de documentos por sucursal, año y mes
doc_pipeline = [
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
            "_id": {"store_name": "$store_name", "year": {"$year": "$date"}, "month": {"$month": "$date"}},
            "total_sales": {"$sum": "$sub_total"}
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
    key = (sale["_id"]["store_name"], sale["_id"]["year"], sale["_id"]["month"])

    if key not in combined_data:
        combined_data[key] = {
            "local": sale["_id"]["store_name"],
            "year": sale["_id"]["year"],
            "month": sale["_id"]["month"],
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
csv_file_path = r"C:\Users\siste\OneDrive\Escritorio\Reportes Python\Mozart\reporte_gasto_produccion.csv"
df.to_csv(csv_file_path, index=False)

# Guardar el DataFrame en formato Excel
excel_file_path = r"C:\Users\siste\OneDrive\Escritorio\Reportes Python\Mozart\reporte_gasto_produccion.xlsx"
df.to_excel(excel_file_path, index=False)

print(f"Reporte guardado en formato CSV: {csv_file_path}")
print(f"Reporte guardado en formato Excel: {excel_file_path}")
