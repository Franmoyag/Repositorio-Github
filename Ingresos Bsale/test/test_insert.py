import mysql.connector
from mysql.connector import Error

def test_insert():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='api_myst',
            user='api_user',
            password='kami.2024.sushi'
        )

        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
            INSERT INTO bsale (
                month, year, codeSii, clientCode, clientActivity, netAmount, iva, ivaAmount, totalAmount, bookType, urlPdf, urlXml
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            data = (
                "1", "2024", "33", "97004000-5", "Banco de Chile", 24498.0, 19.0, 4655.0, 29153.0, "compra",
                "http://dte.bsale.cl/get_dte/42255253?sign=18cd97ca7cee9dab", 
                "http://dte.bsale.cl/get_dte/42255253.xml?sign=18cd97ca7cee9dab"
            )

            cursor.execute(insert_query, data)
            connection.commit()
            print("Inserción exitosa.")

    except Error as e:
        print(f"Error al conectar a MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada")

test_insert()
