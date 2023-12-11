# File location and type
file_location = "/FileStore/tables/Ventas.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Ventas"

df.createOrReplaceTempView(temp_table_name)
-------------------------------------------------------------------------------------------------------------------------------------------------------

# File location and type
file_location = "/FileStore/tables/Productos.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Productos"

df.createOrReplaceTempView(temp_table_name)


-------------------------------------------------------------------------------------------------------------------------------------------------------

# File location and type
file_location = "/FileStore/tables/Productos.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Productos"

df.createOrReplaceTempView(temp_table_name)
-------------------------------------------------------------------------------------------------------------------------------------------------------

# File location and type
file_location = "/FileStore/tables/Clientes.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Clientes"

df.createOrReplaceTempView(temp_table_name)

-------------------------------------------------------------------------------------------------------------------------------------------------------
# File location and type
file_location = "/FileStore/tables/Ubicaciones.CSV"
file_type = "CSV"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table

temp_table_name = "Ubicaciones"

df.createOrReplaceTempView(temp_table_name)

-------------------------------------------------------------------------------------------------------------------------------------------------------

%sql

CREATE OR REPLACE TEMP VIEW datos_supertienda AS
SELECT 
v.ID,
v.OrdenID,v.FechaOrden,v.FechaEnvio,v.ModoEnvio,v.ClienteID,v.CodigoPostal,v.ProductoID,v.Ventas,v.Cantidad,v.Descuento,v.Ganancia,c.cliente,c.segmento,p.producto,p.categoria,p.subcategoria,u.ciudad,u.estado,u.region,u.pais,
CASE WHEN v.Cantidad != 0 THEN (v.Ventas - v.Ganancia) / v.Cantidad ELSE 0 END AS CostoUnitario,
CASE WHEN (v.Ventas - v.Ganancia) != 0 THEN v.Ganancia / (v.Ventas - v.Ganancia) ELSE 0 END AS Recargo,
(v.Ventas / v.Cantidad) / (1 - v.Descuento) AS PrecioUnitario,
v.Ventas - v.Ganancia AS CostoTotal,
DATEDIFF(v.FechaEnvio, v.FechaOrden) AS DemoraEnvio

FROM `Ventas` v
LEFT JOIN `Clientes` c ON v.ClienteID = c.ClienteID
LEFT JOIN `Productos` p ON v.ProductoID = p.ProductoID
LEFT JOIN `Ubicaciones` u ON v.CodigoPostal = u.CodigoPostal
-------------------------------------------------------------------------------------------------------------------------------------------------------
%sql

SELECT * FROM datos_supertienda


-------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Listado de productos más vendidos
SELECT producto, COUNT(producto) as CantidadVentas
FROM datos_supertienda
GROUP BY producto
ORDER BY CantidadVentas DESC
LIMIT 10
-------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Cantidad de ventas por categoría
SELECT categoria, COUNT(categoria) as CantidadVentas
FROM datos_supertienda
GROUP BY categoria
ORDER BY CantidadVentas DESC
-------------------------------------------------------------------------------------------------------------------------------------------------------

#Demora con PySpark
df = spark.sql("SELECT DemoraEnvio FROM datos_supertienda")
df.describe(['DemoraEnvio']).show()
-------------------------------------------------------------------------------------------------------------------------------------------------------

ventas_ganancias_region_df = spark.sql("""
    SELECT region, SUM(Ventas) as VentasTotales, SUM(Ganancia) as GananciasTotales
    FROM datos_supertienda
    GROUP BY region
""").toPandas()
-------------------------------------------------------------------------------------------------------------------------------------------------------

import seaborn as sns
import matplotlib.pyplot as plt

#Ventas y ganancias por region
#Gráfico
plt.figure(figsize=(12, 6))
bar1 = sns.barplot(x='region', y='VentasTotales', data=ventas_ganancias_region_df, label='Ventas Totales')
bar2 = sns.barplot(x='region', y='GananciasTotales', data=ventas_ganancias_region_df, label='Ganancias Totales', color='r', alpha=0.5)

#Etiquetas de datos
for p in bar1.patches:
    height = p.get_height()
    plt.text(p.get_x() + p.get_width()/2., height + 3, '{:1.2f}'.format(height), ha="center", va='bottom')

for p in bar2.patches:
    height = p.get_height()
    plt.text(p.get_x() + p.get_width()/2., height + 3, '{:1.2f}'.format(height), ha="center", va='bottom')

#Título y etiquetas
plt.title('Comparación de Ventas y Ganancias por Región')
plt.xlabel('Región')
plt.ylabel('Totales')
plt.legend()
plt.show()
-------------------------------------------------------------------------------------------------------------------------------------------------------

ventas_tiempo_df = spark.sql("""
    SELECT CONCAT(FORMAT_STRING('%04d', YEAR(FechaOrden)), FORMAT_STRING('%02d', MONTH(FechaOrden))) as Periodo, SUM(Ventas) as VentasTotales
    FROM datos_supertienda
    GROUP BY Periodo
    ORDER BY Periodo
""").toPandas()
-------------------------------------------------------------------------------------------------------------------------------------------------------

import matplotlib.pyplot as plt
#Ventas en el tiempo
plt.figure(figsize=(18, 6))
plt.plot(ventas_tiempo_df['Periodo'], ventas_tiempo_df['VentasTotales'], marker='o')
plt.title('Tendencias de Ventas por mes')
plt.xlabel('Periodo')
plt.ylabel('Ventas Totales')
plt.xticks(ventas_tiempo_df['Periodo'], rotation=45)  
plt.grid(True)
plt.show()

-------------------------------------------------------------------------------------------------------------------------------------------------------

df_compras_spark = spark.sql("""
    WITH compras_ordenadas AS (
        SELECT 
            v.ClienteID,
            c.segmento,
            v.FechaOrden,
            ROW_NUMBER() OVER (PARTITION BY v.ClienteID ORDER BY v.FechaOrden) as orden_compra
        FROM datos_supertienda v
        JOIN Clientes c ON v.ClienteID = c.ClienteID
    ),
    compras_primera_segunda AS (
        SELECT 
            a.ClienteID,
            a.segmento,
            a.FechaOrden as PrimeraCompra,
            b.FechaOrden as SegundaCompra
        FROM compras_ordenadas a
        LEFT JOIN compras_ordenadas b ON a.ClienteID = b.ClienteID AND b.orden_compra = 2
        WHERE a.orden_compra = 1
    )
    SELECT 
        ClienteID,
        segmento,
        PrimeraCompra,
        SegundaCompra,
        CASE 
            WHEN SegundaCompra IS NOT NULL THEN DATEDIFF(SegundaCompra, PrimeraCompra)
            ELSE NULL 
        END as DiferenciaDias
    FROM compras_primera_segunda
""")
-------------------------------------------------------------------------------------------------------------------------------------------------------

df_compras = df_compras_spark.toPandas()

-------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
import matplotlib.pyplot as plt

# Suponiendo que df_compras es tu DataFrame
segmentos = df_compras.groupby('segmento')['DiferenciaDias']
estadisticas = segmentos.describe()
estadisticas = estadisticas[['count', 'mean']]
print(estadisticas)

-------------------------------------------------------------------------------------------------------------------------------------------------------

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(compras_segmento_df['segmento'], compras_segmento_df['TotalCompras'])
plt.title('Total de Compras por Segmento de Clientes')
plt.xlabel('Segmento de Cliente')
plt.ylabel('Número Total de Compras')
plt.xticks(rotation=45)
plt.show()
-------------------------------------------------------------------------------------------------------------------------------------------------------

# Query dcto y rentabilidad
df_datos = spark.sql("""
    SELECT 
       CAST(Descuento as float), 
        CAST(Ganancia as float), 
        CAST(CostoUnitario as float), 
       CAST( PrecioUnitario as float)
    FROM 
        datos_supertienda
""").toPandas()

-------------------------------------------------------------------------------------------------------------------------------------------------------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Asegurando que Descuento es de tipo categórico y Ganancia es numérico
df_datos['Descuento'] = pd.to_numeric(df_datos['Descuento'], errors='coerce')
df_datos['Ganancia'] = pd.to_numeric(df_datos['Ganancia'], errors='coerce')

# Calcular el beneficio promedio por nivel de descuento
beneficio_promedio_por_descuento = df_datos.groupby('Descuento')['Ganancia'].mean().reset_index()

# Crear un gráfico de líneas para mostrar el beneficio promedio por nivel de descuento
plt.figure(figsize=(12, 8))
sns.lineplot(x='Descuento', y='Ganancia', data=beneficio_promedio_por_descuento, marker='o')

plt.title('Beneficio Promedio por Nivel de Descuento')
plt.xlabel('Descuento (%)')
plt.ylabel('Beneficio Promedio ($)')

# Mostrar los descuentos en el eje de las x
plt.xticks(beneficio_promedio_por_descuento['Descuento'].unique())

plt.grid(True)
plt.show()





