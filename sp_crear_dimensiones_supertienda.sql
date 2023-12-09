ALTER PROCEDURE sp_crear_dimensiones_supertienda
AS
BEGIN
    SET NOCOUNT ON;

--TABLA PRODUCTOS	
    BEGIN TRY
    -- Insertar productos nuevos que no existan en la tabla de dimensiones.
    INSERT INTO [DWH_bootcamp_bix].[dbo].[Productos] (ProductoID, Producto, Categoria, Subcategoria)
	SELECT * FROM
  (SELECT 
    [Product ID] + CASE 
                      WHEN COUNT(*) OVER (PARTITION BY [Product ID]) > 1 THEN 
                          '-' + CAST(ROW_NUMBER() OVER (PARTITION BY [Product ID] ORDER BY [Product Name]) AS VARCHAR) 
                      ELSE 
                          '' 
                   END AS ProductoID,
				      [Product Name] as Producto,
					  Category as Categoria,
					  [Sub-Category] as Subcategoria
FROM 
    [data_lake].[dbo].[supertienda]
GROUP BY 
    [Product ID], [Product Name], Category,[Sub-Category]) a WHERE a.ProductoID

        NOT IN (SELECT ProductoID FROM [DWH_bootcamp_bix].[dbo].[Productos]);
	
		-- Registrar el éxito en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_productos', 'Exito', NULL);
    END TRY
    BEGIN CATCH
        -- Registrar el error en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_productos', 'Error', ERROR_MESSAGE());
    END CATCH;


--TABLA UBICACIONES
	 BEGIN TRY
    -- Insertar ubicaciones nuevas que no existan en la tabla de dimensiones.
    INSERT INTO [DWH_bootcamp_bix].[dbo].[Ubicaciones] (CodigoPostal,Ciudad,Estado,Region,Pais)
	SELECT * FROM
  (SELECT 
    [Postal Code] + CASE 
                      WHEN COUNT(*) OVER (PARTITION BY [Postal Code]) > 1 THEN 
                          '-' + CAST(ROW_NUMBER() OVER (PARTITION BY [Postal Code] ORDER BY [City]) AS VARCHAR) 
                      ELSE 
                          '' 
                   END AS CodigoPostal,
				      [City] as Ciudad,
					  [State] as Estado,
					  [Region] as Region,
					  Country as Pais
FROM 
    [data_lake].[dbo].[supertienda]
GROUP BY 
    [Postal Code],[City], [State],[Region],Country) a WHERE a.CodigoPostal

        NOT IN (SELECT CodigoPostal FROM [DWH_bootcamp_bix].[dbo].[Ubicaciones]);
	
		-- Registrar el éxito en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_ubicaciones', 'Exito', NULL);
    END TRY
    BEGIN CATCH
        -- Registrar el error en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_ubicaciones', 'Error', ERROR_MESSAGE());
    END CATCH;


	--TABLA CLIENTES
	 BEGIN TRY
    -- Insertar clientes nuevos que no existan en la tabla de dimensiones.
    INSERT INTO [DWH_bootcamp_bix].[dbo].[Clientes] (ClienteID,Cliente,Segmento)
	SELECT * FROM
  (SELECT DISTINCT [Customer ID],[Customer Name], Segment FROM [data_lake].[dbo].[supertienda]) a WHERE a.[Customer ID]

        NOT IN (SELECT ClienteID FROM [DWH_bootcamp_bix].[dbo].[Clientes]);
	
		-- Registrar el éxito en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_clientes', 'Exito', NULL);
    END TRY
    BEGIN CATCH
        -- Registrar el error en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_crear_dimensiones_supertienda_clientes', 'Error', ERROR_MESSAGE());
    END CATCH;

END;
