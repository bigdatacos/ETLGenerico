from Imports import *
from ETL.src.utils import * 

ip   = ''
port = ""
bbdd = ''

ruta = r''
hoja = ''

connection = mysql_connection(ip,port,bbdd)
df = pd.read_excel(ruta,sheet_name=hoja)
# df = pd.read_excel(ruta,sheet_name='')

# df = df.rename(columns = {
#        "Number of retries ": "Number of retries"})  

# columnas_hora = df.select_dtypes(include=['timedelta64']).columns
# lista_columnas_hora = list(columnas_hora)

# if len(lista_columnas_hora) == 0:
#         pass
# else:
#         for i in lista_columnas_hora:
#                 df[i] = df[i].astype(str).map(lambda x: x[7:])    

# columnas_fecha = df.select_dtypes(include=['datetime64']).columns
# lista_columnas_fecha = list(columnas_fecha)

# if len(lista_columnas_fecha) == 0:
#         pass
# else:
#         for j in lista_columnas_fecha:
#                 df[j] = df[j].apply(lambda x: datetime.datetime.strptime(str(x),"%Y/%m/%d"))

fechas = []
for i in fechas:
       # df[i]=df[i].apply(lambda x: datetime.datetime.strptime(str(x),"%d/%m/%Y"))
       df[i] = pd.to_datetime(df[i], errors='coerce')
       # df[i] = df[i].apply(lambda x: datetime.datetime.strptime(str(x),"%d/%m/%Y") if x is not None else None)
       


df.to_sql("tb_plantilla_hc_tmp",connection,None,'append',10000)


# Cerrar la conexión
connection.close()
