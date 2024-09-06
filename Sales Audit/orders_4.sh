#!/bin/bash


# directorio
cd /root/myenv/

#activacion de entorno virtual
source bin/activate

#directorio de archivos
cd /root/files/sales_audit

#ejecucion de recuparacion de datos.
python3 orders_4.py

#desactivar entorno virtual.
deactivate