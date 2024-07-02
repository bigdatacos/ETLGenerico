import os
import sys
import csv
import time
import yaml
import json
import pyodbc
import pymysql
import logging
import openpyxl
import numpy as np
import pandas as pd
import logging.config
from paths import *
from datetime import datetime,timedelta
from urllib.parse import quote
from sqlalchemy import Table, MetaData, create_engine, Column,text,engine
sys.path.append(path_to_config)
from credentials import *

with open(os.path.join(path_to_config,"logger.yml")) as f:
    logging.config.dictConfig(yaml.safe_load(f))
  
def mysql_engine(ip:str,port:str,bbdd:str)-> engine:
    """Creacion de motor de MySQL para generar conexiones y acceso a metadata segun la base de datos obtenida

    Args:
        ip (str): IP de instancia de MySQL destino de conexion
        port (str): Puerto de instancia de MySQL de destino de conexion
        bbdd (str): Base de datos donde se dea hacer la conexion

    Returns:
        Engine: Motor de MySQL. Solo se accede a la metadata de la base de datos ingresada.
    """    
    return  create_engine(f'mysql+pymysql://{dict_user.get(ip)}:{quote(dict_pwd.get(ip))}@{ip}:{port}/{bbdd}',pool_recycle=9600,isolation_level="AUTOCOMMIT")

def mssql_engine(ip: str, port: str, bbdd: str) -> engine:
    """Creación de motor de MSSQL para generar conexiones y acceso a metadata según la base de datos obtenida

    Args:
        ip (str): IP de instancia de MSSQL destino de conexión
        port (str): Puerto de instancia de MSSQL de destino de conexión
        bbdd (str): Base de datos donde se desea hacer la conexión

    Returns:
        Engine: Motor de MSSQL. Solo se accede a la metadata de la base de datos ingresada.
    """
    return create_engine(f"mssql+pyodbc://{dict_user.get(ip)}:{quote(dict_pwd.get(ip))}@{ip}:{port}/{bbdd}?driver=ODBC+Driver+17+for+SQL+Server", pool_recycle=9600, isolation_level="AUTOCOMMIT")