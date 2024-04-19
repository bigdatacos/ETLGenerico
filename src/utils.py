import os
import sys
import csv
import time
import yaml
import json
import pymysql
import logging
import openpyxl
import numpy as np
import pandas as pd
import logging.config
from paths import *
from datetime import datetime,timedelta
from urllib.parse import quote
from sqlalchemy import Table, MetaData, create_engine, Column,text,Engine
sys.path.append(path_to_config)
from credentials import *

with open(os.path.join(path_to_config,"logger.yml")) as f:
    logging.config.dictConfig(yaml.safe_load(f))

def mysql_engine(ip:str,port:str,bbdd:str)->Engine:
    """Creacion de motor de MySQL para generar conexiones y acceso a metadata segun la base de datos obtenida

    Args:
        ip (str): IP de instancia de MySQL destino de conexion
        port (str): Puerto de instancia de MySQL de destino de conexion
        bbdd (str): Base de datos donde se dea hacer la conexion

    Returns:
        Engine: Motor de MySQL. Solo se accede a la metadata de la base de datos ingresada.
    """    
    return  create_engine(f'mysql+pymysql://{user_mysql}:{quote(pswd_mysql)}@{ip}:{port}/{bbdd}',pool_recycle=9600,isolation_level="AUTOCOMMIT")
    