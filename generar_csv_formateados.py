import pyodbc
import pandas as pd
import logging
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parámetros de conexión
server = 'DESKTOP-PMEU8TJ\SQLEXPRESS'
database = 'DWH_bootcamp_bix'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Intentar conectar a SQL Server
try:
    conn = pyodbc.connect(conn_str)
    logging.info("Conexión exitosa a la base de datos.")
except Exception as e:
    logging.error(f"Error al conectar a la base de datos: {e}")
    exit()

# Consultas SQL
queries = {
    'Clientes': 'SELECT * FROM [dbo].[Clientes]',
    'Productos': 'SELECT * FROM [dbo].[Productos]',
    'Ubicaciones': 'SELECT * FROM [dbo].[Ubicaciones]',
    'Ventas': 'SELECT * FROM [dbo].[ventas_supertienda]'
}

# Carpeta de destino para los archivos CSV
csv_folder_path = r'C:\Users\Usuario\Documents\DatosSupertienda\CSV'

# Asegurarse de que la carpeta de destino existe
if not os.path.exists(csv_folder_path):
    logging.info(f"Creando carpeta: {csv_folder_path}")
    os.makedirs(csv_folder_path)

# Ejecutar cada consulta y guardar los resultados en archivos CSV
for name, query in queries.items():
    try:
        logging.info(f"Procesando consulta: {name}")
        df = pd.read_sql(query, conn)
        csv_path = f'{csv_folder_path}\\{name}.CSV'
        df.to_csv(csv_path, index=False)
        logging.info(f"Archivo creado: {csv_path}")
    except Exception as e:
        logging.error(f"Error al procesar la consulta {name}: {e}")

# Cerrar la conexión
conn.close()
logging.info("Conexión cerrada. Proceso finalizado.")
