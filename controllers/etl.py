import os
import sys
sys.path.append(r"C:\dev\ETL\src")
from ETL import *

ip_or        = '172.10.7.224'
port_or      = '3306'
bbdd_or      = 'bbdd_config'

ip_des       = '172.70.7.61'
port_des     = '3306'
bbdd_des     = 'bbdd_config'

table_name   = 'tb_eventos_torniquetes_groupcos'
column_name  = 'fecha_hora' 
fecha_inicio = '2024-03-01 00:00:00'
fecha_fin    = '2024-04-20 00:00:00'

ETLcomplete(1,ip_or,port_or,bbdd_or,ip_des,port_des,bbdd_des,table_name,column_name,fecha_inicio,fecha_fin)