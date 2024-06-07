from utils import *

ini = time.time()

ip   = ''
port = ''
bbdd = ''

type_file = ''

engine_or = mysql_engine(ip,port,bbdd)

nombre_archivo = ''
with open(os.path.join(path_to_sql,"query_to_export.sql"), 'r',encoding='utf-8') as file:
    sql = file.read()
    sql = sql.format(ip=ip)


logging.getLogger("user").info(f"[ {'EXPORTING':}: {os.path.join(path_to_export,nombre_archivo)} >> origin: {bbdd}@{ip}:{port} ]")
try:
    with engine_or.connect() as conn:
        df  = pd.read_sql(text(sql),conn)
    if type_file == 'excel':
        df.to_excel(os.path.join(path_to_export,f"{nombre_archivo}.xlsx"))
    elif type_file == 'csv':
        df.to_csv(os.path.join(path_to_export,f"{nombre_archivo}.csv"),sep=';',index=False)

    logging.getLogger("user").info(f"[ EXPORT COMPLETE: {os.path.join(path_to_export,nombre_archivo)} >> origin: {bbdd}@{ip}:{port} >> {time.time() - ini:.2f} sec >> {df.shape[0]} rows >> {df.shape[1]} columns ]")
except ValueError as e:
    logging.getLogger("dev").error(f"Error : {e}")