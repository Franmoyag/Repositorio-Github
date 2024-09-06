#!/bin/bash

sudo systemctl restart mongod

# directorio
cd /root/myenv/

#activacion de entorno virtual
source bin/activate

#directorio de archivos
cd /root/files/sales_audit

#ejecucion de recuparacion de datos.
python3 differences_transfer.py

#desactivar entorno virtual.
deactivate