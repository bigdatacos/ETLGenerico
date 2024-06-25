from utils import *
# from ETL.src.utils import * 

ip   = '172.10.7.224'
port = "3306"
bbdd = 'bbdd_config'

table_name = 'tb_informe_cos_performance_metadata'
ruta =fr'{path_to_export}/{table_name}.json'
       
engine_des = mysql_engine(ip,port,bbdd)

sql = """SELECT 
    row_number() over (order by campana asc) as cid, campana,`database` as bbdd_des, target_host as ip_des, target_port as port_des
FROM
    bbdd_config.tb_informe_cos_performance_metadata;"""

with engine_des.connect() as conn:
    df = pd.read_sql(text(sql),conn)
df.to_json(ruta,orient="records",index=False,indent=2)
