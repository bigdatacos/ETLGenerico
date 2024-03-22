from utils import *

ini = time.time()

ip   = '172.17.8.68'
port = '3306'
bbdd = 'bbdd_groupcos_dba'

engine_or = mysql_engine(ip,port,bbdd)

# Ejecutar la consultas

table_name  = 'tb_calendario_day_intervalo_15'
column_name = '' 
name_file = f''
type = "" # TODO: tipo de archivo excel or csv
fecha_inicio = ''
fecha_fin    = ''

sql = f"SELECT * FROM {table_name} WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_inicio}';" if fecha_inicio and fecha_fin else f"SELECT * FROM {table_name};"
try:
    with engine_or.connect() as conn:
        df  = pd.read_sql(sql,conn)
    if type == 'excel':
        df.to_excel(os.path.join(path_to_export,f"{name_file}.xlsx"))
    elif type == 'csv':
        df.to_csv(os.path.join(path_to_export,f"{name_file}.csv"),sep=';')

    logging.getLogger("user").info(f"[ EXPORT COMPLETE : file:{os.path.join(path_to_export,name_file)} >> {table_name} >> origin: {bbdd}@{ip}:{port} >> date range: ( {fecha_inicio} - {fecha_fin} ) >> {time.time() - ini:.2f} sec >> {df.shape[0]} rows >> {df.shape[1]} columns ]")
except Exception as e:
    logging.getLogger("dev").debug(f"Error : {e}")