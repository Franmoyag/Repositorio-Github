import mysql.connector
from mysql.connector import Error

def test_insert_details():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='api_myst',
            user='api_user',
            password='kami.2024.sushi'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Insertar un registro en la tabla bsale para obtener un bsale_id válido
            insert_bsale_query = """
            INSERT INTO bsale (
                month, year, codeSii, clientCode, clientActivity, netAmount, iva, ivaAmount, totalAmount, bookType, urlPdf, urlXml
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            bsale_data = (
                "1", "2024", "33", "97004000-5", "Banco de Chile", 24498.0, 19.0, 4655.0, 29153.0, "compra",
                "http://dte.bsale.cl/get_dte/42255253?sign=18cd97ca7cee9dab", 
                "http://dte.bsale.cl/get_dte/42255253.xml?sign=18cd97ca7cee9dab"
            )
            cursor.execute(insert_bsale_query, bsale_data)
            bsale_id = cursor.lastrowid

            # Prueba de inserción en la tabla bsale_details
            detail_insert_query = """
            INSERT INTO bsale_details (
                bsale_id, NroLinDet, TpoCodigo, VlrCodigo, NmbItem, QtyItem, PrcItem, MontoItem
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            detail_data = (
                bsale_id, 1, "Código de prueba", "Valor de código", "Nombre del ítem", 10.0, 100.0, 1000.0
            )

            cursor.execute(detail_insert_query, detail_data)
            connection.commit()
            print("Inserción exitosa en bsale_details.")

    except Error as e:
        print(f"Error al conectar a MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada")

test_insert_details()
