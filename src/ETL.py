from utils import *

class the_etl:

    # * Funcion de ETL generica
    def ETLcomplete(ip_or:str,port_or:int,bbdd_or:str,ip_des:str,port_des:int,bbdd_des:str,table_name_or:str,table_name_des:str,column_name:str,fecha_inicio:datetime=None,fecha_fin:datetime=None) -> None:
        """Etl de MysSQL a MySQL para migrar informacion a los servidores de Big Data

        Args:
            ip_or (str): Ip de origen de la instancia MySQL
            port_or (int): Puerto origen de la instancia MySQL
            bbdd_or (str): Base de datos origen de la instancia MySQL
            ip_des (str): IP destino de la instancia MySQL
            port_des (int): Puerto destino de la instancia MySQL
            bbdd_des (str): Base de datos destino de la instancia MySQL
            table_name_or (str): Nombre de la tabla que se quiere migear en la instancia de MySQL de origen
            table_name_des (str): Nombre de la tabla que se quiere migear en la instancia de MySQL de origen
            column_name (str): Nombre de la columna por la cual se desea filtrar
            fecha_inicio (datetime, optional): Filtro de fecha inicio o id desde donde se hara el filtro. Defaults to None.
            fecha_fin (datetime, optional): Filtro de fecha de fin o id desde donde se hara el filtro. (Defaults: columns type date: 2024-04-01 -> type datetime: 2024-04-01 00:00:00 -> type int: 1).
        Returns:
            None
        """        
        ini = time.time()
        try:
            engine_or  = mysql_engine(ip_or,port_or,bbdd_or)
            engine_des = mysql_engine(ip_des,port_des,bbdd_des)
            sql = f"SELECT * FROM {table_name_or} WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_fin}';" if fecha_inicio and fecha_fin else f"SELECT * FROM {table_name_or};"
            logging.getLogger("user").debug(sql)
            logging.getLogger("user").info(f"[ START: origin: {bbdd_or}@{ip_or}:{port_or} -> target: {bbdd_des}@{ip_des}:{port_des} ]")
            logging.getLogger("user").info(f"[ TABLE: {table_name_or} >> column '{column_name}' range: ( {fecha_inicio if fecha_inicio else '*'} - {fecha_fin if fecha_fin else '*'} ) ]")

            try:
                with engine_or.connect() as conn_or:
                    df = pd.read_sql(sql,conn_or)
                logging.getLogger("user").debug(f"Dataframe obtenido -> {df.shape[0]} registros")
                if not df.empty:
                    for i in list(df.select_dtypes(include=['timedelta64']).columns):
                        df[i] = df[i].astype(str).map(lambda x: x[7:])    
                    tabla_real = Table(table_name_des, MetaData(), autoload_with = engine_des if ip_or not in dict_serv_bigdata else engine_or )
                    tabla_real.create(bind=engine_des,checkfirst=True)
                    columnas_nuevas = [Column(c.name, c.type) for c in tabla_real.c]
                    tmp = Table(f"{table_name_des}_tmp", MetaData(), *columnas_nuevas)
                    tabla_real.create(bind = engine_des,checkfirst=True)
                    tmp.drop(bind = engine_des,checkfirst=True)
                    tmp.create(bind = engine_des)
                    logging.getLogger("user").debug(f"Matando querys toxicas")
                    the_etl.kill_processes(ip_des,port_des,bbdd_des,table_name_des)
                    with engine_des.connect() as conn_des:
                        logging.getLogger("user").debug(f"Insertando datos en tabla temporal: {table_name_des}_tmp")
                        df.to_sql(f"{table_name_des}_tmp", conn_des, if_exists='append',index=False,chunksize=100000)
                        logging.getLogger("user").debug(f"Ejecutando replace en: {table_name_des}")
                        conn_des.execute(text(f"REPLACE INTO `{tabla_real.name}` SELECT * FROM `{tmp.name}`;"))
                    tmp.drop(bind = engine_des)
                    logging.getLogger("user").info(f"[ SUCCESS -> {table_name_des} : {df.shape[0]} rows >> {df.shape[1]} columns  >> {time.time()-ini:.2f} sec ]\n")
                else:
                    logging.getLogger("user").info(f"[ EMPTY DATAFRAME: {table_name_or} >> or: {bbdd_or}@{ip_or}:{port_or} -> des: {bbdd_des}@{ip_des}:{port_des} ]\n")
            except ValueError as e:
                logging.getLogger("dev").error(f"Error : {e}")
        except ValueError as e:
            logging.getLogger("dev").error(f"Error : {e}")

    # * Funcion para obtener el ultimo registro filtrando dentro de una tabla y columna especifica
    def get_last_row(table_name:str, column_name:str, column_type:str, ip:str,port:int,bbdd:str) -> str:
        """ Obtiene el ultimo registro (Maximo) almacenado dentro de una tabla especifica filtrando por la columna asignada 

        Args:
            table_name (str): Nombre de la tabla
            column_name (str): Columna asiganada para filtrar la información
            ip (str): IP de instancia de MySQl donde se revisara la tabla

        Returns:
            str: Ultimo registro dentro de la tabla, sea un tipo fecha hora o id
        """    
        try: 
            engine_destino = mysql_engine(ip,port,bbdd)
            with engine_destino.connect() as conn:
                sql = f"SELECT `{column_name}` FROM `{table_name}` ORDER BY `{column_name}` DESC LIMIT 1;"
                logging.getLogger("user").debug(sql)
                df = pd.read_sql(sql,conn)
            if not df.empty:
                if column_type == 'datetime':
                    last_row = df.iloc[0,0] - timedelta(hours = 2)
                elif column_type == 'date':
                    last_row = df.iloc[0,0] - timedelta(days = 30)
                else:
                    last_row = df.iloc[0,0]
                return last_row
            else:
                if column_type == 'datetime':
                    last_row = '2024-04-01 00:00:00'
                elif column_type == 'date':
                    last_row = '2024-04-01'
                else:
                    last_row = 1
                return last_row
            
        except Exception as e:
            logging.getLogger("dev").error(f"{table_name} -> {e}")
            last_row = 1 if column_name == 'id' else '2024-04-15 00:00:00'
            logging.getLogger("user").debug(f"Can't obtain maximum of {table_name}. Standard: {last_row}")
            return last_row
        finally:    
            logging.getLogger("user").debug(f"Last row in {ip} -> {table_name} >> {last_row}")

    # * Funcion para matar querys que impiden la ejecucion del replace sobre una tabla
    def kill_processes(ip_des,port_des,bbdd_des,table_name):
        with open(os.path.join(path_to_sql,"kill_query.sql"),'r') as f:
            kill = f.read()
            kill_query = kill.format(usuario=dict_user.get(ip_des),table_name=table_name)
        try:
            engine_destino = mysql_engine(ip_des,port_des,bbdd_des)
            with engine_destino.connect() as conn_des:
                df = pd.read_sql(text(kill_query),conn_des)
            ids = df['id'].tolist()                    
            for i in ids:
                with engine_destino.connect() as conn_des:
                    try:
                        conn_des.execute(text(f"KILL {i}"))
                    except:
                        pass
                logging.getLogger("dev").error(f"Matando : {i}")
        
        except ValueError as e:
            logging.getLogger("dev").error(e)
            with engine_destino.connect() as conn_des:
                df = pd.read_sql(text(kill_query),conn_des)
            ids = df['id'].tolist()                    
            for i in ids:
                with engine_destino.connect() as conn_des:
                    try:
                        conn_des.execute(text(f"KILL {i}"))
                    except:
                        pass
                logging.getLogger("dev").error(f"Matando : {i}")

class the_execution:

    # * Funcion de mostrar ayuda
    def show_help():
        with open(os.path.join(path_to_docs,"documentation.txt"),'r') as file:
            print(file.read())

    # * Funcion para leer el archivo .json con las tablas a ejecutar
    def data_to_run(file) -> json:
        """Lectura de archivo JSON con los elementos a ejecutar en proceso de ETL

        Returns:
            json: Cadena de texto tipo json con los valores a ejecutar
        """    
        with open(os.path.join(path_to_data,f'{file}.json')) as file:
            return json.load(file)

    # * Funcion para listar tablas con su respectivo cid 
    def list_cid_tables():
        print(f"[ {'Table':^35} |{'origin (IP:Port) -> target (IP:Port)':^42}| {'Column Type':12} |{'CID':^7}]\n[{'-'*103}]")
        [ print(f"[ {i['table_name_or']:35} | {i['ip_or']:>12}:{i['port_or']:<5} -> {i['ip_des']:>12}:{i['port_des']:<5} | {i['column_type']:^12} | {i['cid']:^5} ]") for i in the_execution.data_to_run("data_to_run")]
 
    # * Funcion de ejecucion mediante cid
    def exec_by_cid():
        cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
        for i in the_execution.data_to_run("data_to_run"):
            if i["cid"] == cid:
                if i["column_type"] == 'datetime':
                    fecha_inicio = sys.argv[3] + " " + sys.argv[4] if len(sys.argv) > 4 else the_etl.get_last_row(i['table_name_des'],i['column_name'],i['column_type'],i['ip_des'],i['port_des'],i['bbdd_des'])
                    fecha_fin    = sys.argv[5] + " " + sys.argv[6] if len(sys.argv) > 6 else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elif i["column_type"] in ['date','id']:
                    fecha_inicio = sys.argv[3] if len(sys.argv) > 3 else the_etl.get_last_row(i['table_name_des'],i['column_name'],i['column_type'],i['ip_des'],i['port_des'],i['bbdd_des'])
                    if i["column_type"] == 'id':
                        fecha_fin = sys.argv[4] if len(sys.argv) > 4 else 50000
                    else:
                        fecha_fin = sys.argv[4] if len(sys.argv) > 4 else datetime.now().strftime("%Y-%m-%d")
                the_etl.ETLcomplete(i['ip_or'],i['port_or'],i['bbdd_or'],i['ip_des'],i['port_des'],i['bbdd_des'],i['table_name_or'],i['table_name_des'],i['column_name'],fecha_inicio=fecha_inicio,fecha_fin=fecha_fin)

    # * Funcion de ejecucion de distro
    def exec_data_auto():
        for i in the_execution.data_to_run("data_to_run"):
            try:
                if i["column_type"] == 'datetime':
                    fecha_inicio = sys.argv[3] + " " + sys.argv[4] if len(sys.argv) > 4 else the_etl.get_last_row(i['table_name_des'],i['column_name'],i['column_type'],i['ip_des'],i['port_des'],i['bbdd_des'])
                    fecha_fin    = sys.argv[5] + " " + sys.argv[6] if len(sys.argv) > 6 else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elif i["column_type"] in ['date','id']:
                    fecha_inicio = sys.argv[3] if len(sys.argv) > 3 else the_etl.get_last_row(i['table_name_des'],i['column_name'],i['column_type'],i['ip_des'],i['port_des'],i['bbdd_des'])
                    if i["column_type"] == 'id':
                        fecha_fin = sys.argv[4] if len(sys.argv) > 4 else 50000
                    else:
                        fecha_fin = sys.argv[4] if len(sys.argv) > 4 else datetime.now().strftime("%Y-%m-%d")
                the_etl.ETLcomplete(i['ip_or'],i['port_or'],i['bbdd_or'],i['ip_des'],i['port_des'],i['bbdd_des'],i['table_name_or'],i['table_name_des'],i['column_name'],fecha_inicio=fecha_inicio,fecha_fin=fecha_fin)
            except ValueError as e:
                logging.getLogger("dev").error(f"{i['ip_des']} >> {i['table_name_or']} >> {e}")
                continue

    # * Diccionario de funciones disponibles para ejecucion del proceso
    dict_actions = {
        '--help'         : show_help,               # TODO: Muestra la ayuda para ejecucion 
        '-h'             : show_help,               # * """"""
        '--list'         : list_cid_tables,         # TODO: lista las tablas que se estan migrando automaticamente
        '-l'             : list_cid_tables,         # * """"""
        '--cid'          : exec_by_cid,             # TODO: ejecuta una tabla en especifico de las tablas que estan automaticas cid
        '-c'             : exec_by_cid,             # * """"""
        '-exe'           : exec_data_auto,          # TODO: execute
        '--execute'      : exec_data_auto           # * """"""
    }

    # * Main de ejecucion dependiente del diccionario
    def execution(action):
        if action in the_execution.dict_actions:
            if isinstance(the_execution.dict_actions[action], list):
                for func in the_execution.dict_actions[action]:
                    try:
                        func()
                    except ValueError as e:
                        logging.getLogger("user").debug(e)
                    finally:
                        continue
            else:
                try:
                    the_execution.dict_actions[action]()
                except ValueError as e:
                    logging.getLogger("user").debug(e)
                finally:
                    pass

        else:
            logging.getLogger("dev").error("Unknown action provided")
