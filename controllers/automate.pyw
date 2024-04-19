import os
import sys
sys.path.append(r"C:\dev\ETL\src")
from utils import *
from ETL import *

# * Funcion de mostrar ayuda
def show_help():
    with open(os.path.join(path_to_share,"documentation.txt"),'r') as file:
        print(file.read())

# * Funcion para leer el archivo .json con las tablas a ejecutar
def data_to_run() -> json:
    """Lectura de archivo JSON con los elementos a ejecutar en proceso de ETL

    Returns:
        json: Cadena de texto tipo json con los valores a ejecutar
    """    
    with open(os.path.join(path_to_data,'data_to_run.json')) as file:
        return json.load(file)

# * Funcion para listar tablas con su respectivo cid 
def list_cid_tables():
    print(f"[ {'Table':^35} |{'origen -> destino':^42}|{'CID':^7}]\n[{'-'*88}]")
    for i in data_to_run():
        print(f"[ {i['table_name']:35} | {i['ip_or']:12}:{i['port_or']:<5} -> {i['ip_des']:12}:{i['port_des']:<5} | {i['cid']:5} ]")

# * Funcion para obtener el ultimo registro filtrando dentro de una tabla y columna especifica
def get_last_id_date(table_name:str, column_name:str, ip:str,port:int,bbdd:str) -> str:
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
            last_row = df.iloc[0,0] - timedelta(hours = 2) if column_name !='id' else df.iloc[0,0]
            return last_row
        else:
            logging.getLogger("user").debug(f"{table_name}  vacia")
            last_row = 1 if column_name == 'id' else '2024-04-15 00:00:00'
            return last_row
    except Exception as e:
        logging.getLogger("dev").error(f"{table_name} -> {e}")
        last_row = 1 if column_name == 'id' else '2024-04-15 00:00:00'
        logging.getLogger("user").debug(f"Can't obtain maximum of {table_name}. Standard: {last_row}")
        return last_row
    finally:    
        logging.getLogger("user").debug(f"Last row in {ip} -> {table_name} >> {last_row}")

# * Funcion de ejecucion mediante cid
def exec_by_cid():
    cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
    for i in data_to_run():
        if i["cid"] == cid:
            fecha_inicio = sys.argv[3] + " " + sys.argv[4] if len(sys.argv) > 4 else get_last_id_date(i['table_name'],i['column_name'],i['ip_des'],i['port_des'],i['bbdd_des'])
            fecha_fin = sys.argv[5] + " " + sys.argv[6] if len(sys.argv) > 6 else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ETLcomplete(**i,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin)

# * Funcion de ejecucion de distro
def load_etl():
    for i in data_to_run():
        fecha_inicio = sys.argv[3] + " " + sys.argv[4] if len(sys.argv) > 4 else get_last_id_date(i['table_name'],i['column_name'],i['ip_des'],i['port_des'],i['bbdd_des'])
        fecha_fin = sys.argv[5] + " " + sys.argv[6] if len(sys.argv) > 6 else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ETLcomplete(**i,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin)

dict_actions = {
    '--help': show_help,
    '-h': show_help,
    '-l': list_cid_tables, # TODO: list
    '-list': list_cid_tables, # TODO: list
    '-c':exec_by_cid, # TODO: cid
    '-exe': [load_etl], # TODO: execute
    '-execute': [load_etl] # TODO: execute
}

# * Main
def execution(action):
    if action in dict_actions:
        [ func() for func in dict_actions[action] ] if isinstance(dict_actions[action], list) else dict_actions[action]()
    else:
        logging.getLogger("dev").error("Unknown action provided")

if __name__ == '__main__':
    execution(sys.argv[1] if len(sys.argv) > 1 else '-h')
