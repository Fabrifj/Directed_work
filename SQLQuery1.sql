USE PlastiForte

DROP TABLE IF EXISTS fact_proformas;

CREATE TABLE fact_proformas(
	Codigo_venta BIGINT  NOT NULL,
	Fecha DATE,
	Codigo_cliente  NVARCHAR(255) NOT NULL,
	Nombre_cliente  NVARCHAR(255) NOT NULL,
	Codigo_vendedor INT,
	Codigo_proyecto NVARCHAR(255),
	Codigo_producto NVARCHAR(255),
	Cantidad_vendida FLOAT,
	Total_proforma FLOAT,
	Total_descuento FLOAT,
	Facturar FLOAT,
	Impuesto FLOAT,
	Total_venta FLOAT,
	Codigo_departamento INT,  
	PRIMARY KEY (Codigo_venta, Codigo_producto),
	FOREIGN KEY (Fecha) REFERENCES dim_time(Fecha),
	FOREIGN KEY (Codigo_departamento) REFERENCES dim_departament(Codigo_departamento)
);


