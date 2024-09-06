import paramiko
from scp import SCPClient
from datetime import datetime
import os

# Obtiene la fecha y hora actuales
now = datetime.now()
actual_month = now.month  # Esto da el mes como un número sin cero inicial, por ejemplo, 8 en lugar de 08
actual_year = now.year

# Configuración del servidor remoto (destino)
host_destino = '67.205.176.183'
puerto_destino = 22
usuario_destino = 'root'
pass_destino = '84tu38PzuHLP'

# Especifica el archivo local y la ubicación de destino en el servidor remoto
nombre_local = f'/root/files/sales_audit/diferencias-KAMI-{actual_month}-{actual_year}.xlsx'
nombre_destino = f'/home/lokaljost/archivos/diferencias-KAMI-{actual_month}-{actual_year}.xlsx'

# Verificar si el archivo local existe antes de intentar transferirlo
if not os.path.exists(nombre_local):
    print(f"Error: El archivo local '{nombre_local}' no existe.")
else:
    try:
        # Conexión SSH al servidor remoto
        ssh_client_destiny = paramiko.SSHClient()
        ssh_client_destiny.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client_destiny.connect(host_destino, port=puerto_destino, username=usuario_destino, password=pass_destino)

        # Transferencia del archivo desde el servidor local al servidor remoto
        with SCPClient(ssh_client_destiny.get_transport()) as scp_destino:
            scp_destino.put(nombre_local, nombre_destino)
            print(f"Archivo '{nombre_local}' transferido exitosamente a '{nombre_destino}'.")

    except Exception as e:
        print(f"Error durante la transferencia: {e}")
    finally:
        # Cierre de la conexión SSH
        ssh_client_destiny.close()
