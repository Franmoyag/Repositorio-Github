import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(
        host='localhost',
        database='api_myst',
        user='api_user',
        password='kami.2024.sushi'
    )

    if connection.is_connected():
        db_info = connection.get_server_info()
        print("Conectado a MySQL Server versión ", db_info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Conectado a la base de datos: ", record)

except Error as e:
    print("Error al conectar a MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Conexión a MySQL cerrada")
