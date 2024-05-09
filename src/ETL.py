from utils import *

def ETLcomplete(cid:int,ip_or:str,port_or:int,bbdd_or:str,ip_des:str,port_des:int,bbdd_des:str,table_name,column_name:str,fecha_inicio:datetime=None,fecha_fin:datetime=None):
    ini = time.time()
    try:
        engine_or = mysql_engine(ip_or,port_or,bbdd_or)
        engine_des = mysql_engine(ip_des,port_des,bbdd_des)
        
        sql = f"SELECT * FROM {table_name} WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_fin}';" if fecha_inicio and fecha_fin else f"SELECT * FROM {table_name};"
        logging.getLogger("user").info(f"[ START: origin: {bbdd_or}@{ip_or}:{port_or} -> target: {bbdd_des}@{ip_des}:{port_des} ]")
        logging.getLogger("user").info(f"[ TABLE: {table_name:40} >> date range: ( {fecha_inicio if fecha_inicio else '*'} - {fecha_fin if fecha_fin else '*'} ) ]")

        try:
            with engine_or.connect() as conn_or:
                df = pd.read_sql(sql,conn_or)
            if not df.empty:
                for i in list(df.select_dtypes(include=['timedelta64']).columns):
                    df[i] = df[i].astype(str).map(lambda x: x[7:])    
                tabla_real = Table(table_name, MetaData(), autoload_with = engine_or)
                tabla_real.create(bind=engine_des,checkfirst=True)
                columnas_nuevas = [Column(c.name, c.type) for c in tabla_real.c]
                tmp = Table(f"{table_name}_tmp", MetaData(), *columnas_nuevas)
                tabla_real.create(bind = engine_des,checkfirst=True)
                tmp.drop(bind = engine_des,checkfirst=True)
                tmp.create(bind = engine_des)
                with engine_des.connect() as conn_des:
                    logging.getLogger("user").debug(f"Insertando datos en tabla temporal: {table_name}_tmp")
                    df.to_sql(f"{table_name}_tmp", conn_des, if_exists='append',index=False,chunksize=100000)
                    logging.getLogger("user").debug(f"Ejecutando replace en: {table_name}")
                    conn_des.execute(text(f"REPLACE INTO `{tabla_real.name}` SELECT * FROM `{tmp.name}`;"))
                tmp.drop(bind = engine_des)
                logging.getLogger("user").info(f"[ SUCCESS -> {table_name} : {df.shape[0]} rows >> {df.shape[1]} columns  >> {time.time()-ini:.2f} sec ]\n")
            else:
                logging.getLogger("user").info(f"[ EMPTY DATAFRAME: {table_name} >> or: {bbdd_or}@{ip_or}:{port_or} -> des: {bbdd_des}@{ip_des}:{port_des} ]\n")
        except Exception as e:
            logging.getLogger("dev").error(f"Error : {e}")

    except Exception as e:
        logging.getLogger("dev").error(f"Error : {e}")


