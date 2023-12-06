import pandas as pd
import pyodbc
import logging

# logging
logging.basicConfig(filename='log_volcado_supertienda.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Parámetros de conexión
server = 'DESKTOP-PMEU8TJ\SQLEXPRESS'  #  servidor
database = 'data_lake'  # base de datos
table_name = 'dbo.supertienda'  #  tabla
excel_file_path = r'C:\Users\Usuario\Documents\DatosSupertienda\Supertienda.xlsx'  # Ruta al archivo Excel

# Cadena de conexión 
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Intenta leer el Excel
try:
    df = pd.read_excel(excel_file_path, header=0)  # header=0 indica que la primera fila contiene los encabezados
    df = df.iloc[:, 1:]  # Omitir la primera columna (rownum)
    df = df.astype(str)  # Convertir todo a varchar
    logging.info("Archivo Excel leído correctamente")
except Exception as e:
    logging.error(f"Error al leer el archivo Excel: {e}")
    raise

# Intenta conectarse a SQL Server
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    logging.info("Conexión a SQL Server establecida")
except Exception as e:
    logging.error(f"Error al conectarse a SQL Server: {e}")
    raise

# Preparar la query para insertar los datos
column_names = ', '.join([f'[{col}]' for col in df.columns])
placeholders = ', '.join('?' * len(df.columns))
sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

# Intenta insertar datos
try:
    for index, row in df.iterrows():
        cursor.execute(sql, tuple(row))
        logging.info(f"Fila {index + 1} insertada")
    conn.commit()
    logging.info(f"Todos los datos insertados correctamente en {table_name}")

    # Insertar un registro de éxito en la tabla control_volcados
    cursor.execute("INSERT INTO dbo.control_volcados (StoreProcedure, Estado) VALUES (?, ?)", 
                   'volcado_python_insercion_supertienda', 'Exito')
    conn.commit()
    logging.info("Registro de éxito insertado en control_volcados")
except Exception as e:
    conn.rollback()
    logging.error(f"Error durante la inserción de datos: {e}")

    # Insertar un registro de error en la tabla control_volcados
    cursor.execute("INSERT INTO dbo.control_volcados (StoreProcedure, Estado, msg) VALUES (?, ?, ?)", 
                   'volcado_python_insercion_supertienda', 'Error', str(e))
    conn.commit()
    logging.error("Registro de error insertado en control_volcados")

# Cerrar la conexión
cursor.close()
conn.close()
logging.info("Conexión a SQL Server cerrada")
