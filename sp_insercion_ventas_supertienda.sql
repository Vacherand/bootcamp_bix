USE [DWH_bootcamp_bix]
GO
/****** Object:  StoredProcedure [dbo].[sp_insercion_ventas_supertienda]    Script Date: 06/12/2023 15:58:27 ******/
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

    -- Llenar la tabla temporal con los datos procesados
    INSERT INTO #tempVentas
    SELECT 
        [Order ID] as OrdenID,
		--Tratamiento de fecha para ORDEN
        CASE
		--Caso YYYY/DD/MM
            WHEN LEN([Order Date]) = 19 THEN
                CAST(SUBSTRING([Order Date], 6, 2) + '/' + 
                SUBSTRING([Order Date], 9, 2) + '/' + 
                LEFT([Order Date], 4) as date)
		--Caso MM/DD/YYYY
            WHEN LEN([Order Date]) = 10 THEN
                CAST(RIGHT([Order Date], 4) + '/' + 
                LEFT([Order Date], 2) + '/' + 
                SUBSTRING([Order Date], 4, 2) as date)
		--Caso M/DD/YYYY
            WHEN LEN([Order Date]) = 9 THEN
                CAST(RIGHT([Order Date], 4) + '/' + 
                '0' + LEFT([Order Date], 1) + '/' + 
                SUBSTRING([Order Date], 3, 2) as date) 
            ELSE NULL
        END AS FechaOrden,
		--Tratamiento de fecha para ENVIO
        CASE	
		--Caso YYYY/DD/MM
            WHEN LEN([Ship Date]) = 19 THEN
                CAST(SUBSTRING([Ship Date], 6, 2) + '/' + 
                SUBSTRING([Ship Date], 9, 2) + '/' + 
                LEFT([Ship Date], 4) as date)
		--Caso MM/DD/YYYY
            WHEN LEN([Ship Date]) = 10 THEN
                CAST(RIGHT([Ship Date], 4) + '/' + 
                LEFT([Ship Date], 2) + '/' + 
                SUBSTRING([Ship Date], 4, 2) as date)
		--Caso M/DD/YYYY
            WHEN LEN([Ship Date]) = 9 THEN
                CAST(RIGHT([Ship Date], 4) + '/' + 
                '0' + LEFT([Ship Date], 1) + '/' + 
                SUBSTRING([Ship Date], 3, 2) as date) 
            ELSE NULL
        END AS FechaEnvio,
        [Ship Mode] as ModoEnvio,
        [Customer ID] as ClienteID,
        [Postal Code] as CodigoPostal,
        [Product ID] as ProductoID,
        CAST(REPLACE(Sales, '~', '.') as float) as Ventas,
        CAST(REPLACE(Quantity, '~', '.') as int) AS Cantidad,
        CAST(REPLACE(Discount, '~', '.') as float) as Descuento,
        CAST(REPLACE(Profit, '~', '.') as float) as Ganancia
    FROM [data_lake].[dbo].[supertienda];

    BEGIN TRY
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
