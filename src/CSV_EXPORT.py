from utils import *

ini = time.time()

ip   = ''
port = ''
bbdd = ''

engine_or = mysql_engine(ip,port,bbdd)

# Ejecutar la consultas

table_name  = ''
column_name = ''
type = "csv" # TODO: tipo de archivo excel or csv
fecha_inicio = ''
fecha_fin    = ''

sql = f"SELECT * FROM {table_name} WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_inicio}';" if fecha_inicio and fecha_fin else f"SELECT * FROM {table_name};"
logging.getLogger("user").info(f"[ {'EXPORTING':15}: {os.path.join(path_to_export,f"{table_name}_{fecha_inicio}_{fecha_fin}")} >> origin: {bbdd}@{ip}:{port} >> date range: ( {fecha_inicio} - {fecha_fin} ) ]")
try:
    with engine_or.connect() as conn:
        df  = pd.read_sql(sql,conn)
    if type == 'excel':
        df.to_excel(os.path.join(path_to_export,f"{table_name}_{fecha_inicio}_{fecha_fin}.xlsx"))
    elif type == 'csv':
        df.to_csv(os.path.join(path_to_export,f"{table_name}_{fecha_inicio}_{fecha_fin}.csv"),sep=';',index=False)

    logging.getLogger("user").info(f"[ EXPORT COMPLETE: {os.path.join(path_to_export,f"{table_name}_{fecha_inicio}_{fecha_fin}")} >> origin: {bbdd}@{ip}:{port} >> date range: ( {fecha_inicio} - {fecha_fin} ) >> {time.time() - ini:.2f} sec >> {df.shape[0]} rows >> {df.shape[1]} columns ]")
except Exception as e:
    logging.getLogger("dev").error(f"Error : {e}")