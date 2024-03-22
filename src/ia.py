from utils import *

# Paso 2: Cargar datos desde el segundo archivo
archivo_segundo = 'ruta/al/segundo/archivo.csv'
datos = pd.read_csv(archivo_segundo)

# Paso 3: Determinar automáticamente el tipo de dato
tipos_de_datos = {col: str(datos[col].dtype) for col in datos.columns}

# Paso 4: Crear la tabla en MySQL con SQLAlchemy
motor = create_engine('mysql+mysqlconnector://tu_usuario:tu_contraseña@tu_host/tu_base_de_datos')
conexion = motor.connect()

nombre_tabla = 'nombre_tabla'
nombre_columna_primaria = 'nombre_columna_primaria'

# Crear la tabla
conexion.execute(f"CREATE TABLE {nombre_tabla} ({', '.join([f'{col} {tipo}' for col, tipo in tipos_de_datos.items()])}, PRIMARY KEY ({nombre_columna_primaria}));")

# Paso 5: Cargar datos en la tabla
datos.to_sql(nombre_tabla, con=conexion, index=False, if_exists='replace')

# Cerrar la conexión
conexion.close()