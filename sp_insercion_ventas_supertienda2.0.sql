USE [DWH_bootcamp_bix]
GO
/****** Object:  StoredProcedure [dbo].[sp_insercion_ventas_supertienda]    Script Date: 06/12/2023 21:38:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_insercion_ventas_supertienda]
AS
BEGIN
    SET NOCOUNT ON;

    -- tabla temporal
    IF OBJECT_ID('tempdb..#tempVentas') IS NOT NULL
        DROP TABLE #tempVentas;

    CREATE TABLE #tempVentas (
        OrdenID NVARCHAR(50),
        FechaOrden DATE,
        FechaEnvio DATE,
        ModoEnvio NVARCHAR(50),
        ClienteID NVARCHAR(50),
        CodigoPostal NVARCHAR(50),
        ProductoID NVARCHAR(50),
        Ventas FLOAT,
        Cantidad INT,
        Descuento FLOAT,
        Ganancia FLOAT
    );

	BEGIN TRY

    -- Llenar la tabla temporal con los datos procesados
    INSERT INTO #tempVentas
    SELECT 
    st.[Order ID] as OrdenID,
    --Tratamiento de fecha para ORDEN
        CASE
		--Caso YYYY/DD/MM
            WHEN LEN(st.[Order Date]) = 19 THEN
                CAST(SUBSTRING(st.[Order Date], 6, 2) + '/' + 
                SUBSTRING(st.[Order Date], 9, 2) + '/' + 
                LEFT([Order Date], 4) as date)
		--Caso MM/DD/YYYY
            WHEN LEN(st.[Order Date]) = 10 THEN
                CAST(RIGHT(st.[Order Date], 4) + '/' + 
                LEFT(st.[Order Date], 2) + '/' + 
                SUBSTRING(st.[Order Date], 4, 2) as date)
		--Caso M/DD/YYYY
            WHEN LEN(st.[Order Date]) = 9 THEN
                CAST(RIGHT(st.[Order Date], 4) + '/' + 
                '0' + LEFT(st.[Order Date], 1) + '/' + 
                SUBSTRING(st.[Order Date], 3, 2) as date) 
            ELSE NULL
        END AS FechaOrden,
		--Tratamiento de fecha para ENVIO
        CASE	
		--Caso YYYY/DD/MM
            WHEN LEN(st.[Ship Date]) = 19 THEN
                CAST(SUBSTRING(st.[Ship Date], 6, 2) + '/' + 
                SUBSTRING(st.[Ship Date], 9, 2) + '/' + 
                LEFT(st.[Ship Date], 4) as date)
		--Caso MM/DD/YYYY
            WHEN LEN(st.[Ship Date]) = 10 THEN
                CAST(RIGHT(st.[Ship Date], 4) + '/' + 
                LEFT(st.[Ship Date], 2) + '/' + 
                SUBSTRING(st.[Ship Date], 4, 2) as date)
		--Caso M/DD/YYYY
            WHEN LEN(st.[Ship Date]) = 9 THEN
                CAST(RIGHT(st.[Ship Date], 4) + '/' + 
                '0' + LEFT(st.[Ship Date], 1) + '/' + 
                SUBSTRING(st.[Ship Date], 3, 2) as date) 
            ELSE NULL
        END AS FechaEnvio,
    st.[Ship Mode] as ModoEnvio,
    st.[Customer ID] as ClienteID,
    u.[CodigoPostal],
    p.ProductoID,
    CAST(REPLACE(st.Sales, '~', '.') as float) as Ventas,
    CAST(REPLACE(st.Quantity, '~', '.') as int) AS Cantidad,
    CAST(REPLACE(st.Discount, '~', '.') as float) as Descuento,
    CAST(REPLACE(st.Profit, '~', '.') as float) as Ganancia
FROM 
    [data_lake].[dbo].[supertienda] st
LEFT JOIN 
    [DWH_bootcamp_bix].[dbo].[Productos] p 
    ON st.[Product ID] = LEFT(p.ProductoID, 15) AND REPLACE(st.[Product Name], '~', '.') = p.Producto
LEFT JOIN 
    [DWH_bootcamp_bix].[dbo].[Ubicaciones] u 
    ON LEFT(st.[Postal Code], 5) = LEFT(u.[CodigoPostal], 5) AND st.City = u.Ciudad;

    
        -- Comprobar si la tabla ventas_supertienda existe
        IF NOT EXISTS (SELECT * FROM DWH_bootcamp_bix.INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_NAME = N'ventas_supertienda')
        BEGIN
            -- Crear la tabla ventas_supertienda y llenar con datos desde la tabla temporal
            CREATE TABLE [DWH_bootcamp_bix].[dbo].[ventas_supertienda] (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                OrdenID NVARCHAR(50),
                FechaOrden DATE,
                FechaEnvio DATE,
                ModoEnvio NVARCHAR(50),
                ClienteID NVARCHAR(50),
                CodigoPostal NVARCHAR(50),
                ProductoID NVARCHAR(50),
                Ventas FLOAT,
                Cantidad INT,
                Descuento FLOAT,
                Ganancia FLOAT
            );

            INSERT INTO [DWH_bootcamp_bix].[dbo].[ventas_supertienda]
            SELECT * FROM #tempVentas;
        END
        ELSE
        BEGIN
            -- Vaciar y llenar la tabla ventas_supertienda con datos nuevos desde la tabla temporal
            TRUNCATE TABLE [DWH_bootcamp_bix].[dbo].[ventas_supertienda];
            INSERT INTO [DWH_bootcamp_bix].[dbo].[ventas_supertienda]
            SELECT * FROM #tempVentas;
        END

        -- Registrar el éxito en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_insercion_ventas_supertienda', 'Exito', NULL);
    END TRY
    BEGIN CATCH
        -- Registrar el error en la tabla control_volcados
        INSERT INTO [data_lake].[dbo].[control_volcados] (StoreProcedure, Estado, msg)
        VALUES ('sp_insercion_ventas_supertienda', 'Error', ERROR_MESSAGE());
    END CATCH;
END;
