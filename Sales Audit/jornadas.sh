#!/bin/bash

sudo systemctl restart mongod

# directorio
cd /root/myenv/

#activacion de entorno virtual
source bin/activate

#directorio de archivos
cd /root/files/sales_audit

#ejecucion de recuparacion de datos.
python3 jornadas.py

#prueba directorio
#cd /root/files/bsale/

#ejecucion de reporte excel.
#python3 report_bsale.py

#desactivar entorno virtual.
deactivate
