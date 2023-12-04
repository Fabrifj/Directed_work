USE PlastiForte
-- Declare the date variable
DECLARE @MyDate DATE;
SET @MyDate = '2016-06-05';

IF NOT EXISTS (SELECT * FROM dim_time WHERE Fecha = @MyDate)
BEGIN
    INSERT INTO dim_time (Fecha,Ano,Trimestre, Mes)
    VALUES (@MyDate, YEAR(@MyDate), DATEPART(QUARTER, @MyDate),MONTH(@MyDate))
END

SELECT * FROM dim_time 
	WHERE Fecha = '2016-05-06'
		OR Fecha = '2016-05-4'

INSERT INTO fact_proformas (Codigo_venta, Fecha, Codigo_proyecto, Codigo_departamento,Codigo_cliente, Codigo_vendedor, Codigo_producto, Cantidad_vendida, Total_proforma, Nombre_cliente, Impuesto, Total_descuento, Facturar,Total_venta)
	VALUES (10009455, '2016-05-06', 'P-005', 6, '03-002', 61.0, 'A350050-1', 97.0, 32291.3, 'GERIMEX_S.R.L.', 4197.869000000001, 0.0, 32291.3, 28093.431)
