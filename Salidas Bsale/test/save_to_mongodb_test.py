from pymongo import MongoClient

def save_to_mongodb(data):
    client = MongoClient('localhost', 27017)
    db = client['api_data']
    collection = db['facturas_2024']
    
    if data:
        collection.insert_many(data)
        print("Datos insertados en MongoDB.")
    else:
        print("No hay datos para insertar en MongoDB.")

if __name__ == "__main__":
    # Datos de prueba
    data = [
        {
            "emissionDate": "2024-01-01",
            "number": 1,
            "codeSii": "33",
            "clientCode": "123",
            "clientActivity": "Retail",
            "netAmount": 1000.0,
            "iva": 190.0,
            "ivaAmount": 190.0,
            "totalAmount": 1190.0,
            "bookType": "Venta",
            "urlPdf": "http://example.com/document1.pdf",
            "urlXml": "http://example.com/document1.xml"
        },
        {
            "emissionDate": "2024-01-02",
            "number": 2,
            "codeSii": "34",
            "clientCode": "124",
            "clientActivity": "Wholesale",
            "netAmount": 2000.0,
            "iva": 380.0,
            "ivaAmount": 380.0,
            "totalAmount": 2380.0,
            "bookType": "Venta",
            "urlPdf": "http://example.com/document2.pdf",
            "urlXml": "http://example.com/document2.xml"
        }
    ]

    save_to_mongodb(data)
