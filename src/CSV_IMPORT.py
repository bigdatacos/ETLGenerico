from utils import *
# from ETL.src.utils import * 

ip   = '172.17.8.68'
port = "3306"
bbdd = 'bbdd_config'

ruta = r'Z:\REPORTING\22_Líder_Reporting\Inventario\90_Inventario.xlsx'
file_type='excel' # * excel o csv

if file_type=='csv':
        df = pd.read_csv(ruta,delimiter=';',encoding='Latin-1')
elif file_type=='excel':
        df = pd.read_excel(ruta,sheet_name='Inventario')

# df = df.rename(columns = {
#        "Number of retries ": "Number of retries"})  


# for i in list(df.select_dtypes(include=['timedelta64']).columns):
#         df[i] = df[i].astype(str).map(lambda x: x[7:])    

# for j in list(df.select_dtypes(include=['datetime64']).columns):
#         df[j] = df[j].apply(lambda x: datetime.datetime.strptime(str(x),"%Y/%m/%d"))

# columnas_de_fecha = []
# for i in columnas_de_fecha:
#        df[i]=df[i].apply(lambda x: datetime.datetime.strptime(str(x),"%d/%m/%Y"))
#        df[i] = pd.to_datetime(df[i], errors='coerce')
       
engine_des = mysql_engine(ip,port,bbdd)
with engine_des.connect() as conn_des:
        df.to_sql("tb_inventario_rp", conn_des, if_exists='append',index=False,chunksize=100000)
