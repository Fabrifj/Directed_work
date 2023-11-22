USE PlastiForte

DROP TABLE IF EXISTS fact_sells;
DROP TABLE IF EXISTS fact_objective
DROP TABLE IF EXISTS dim_client
DROP TABLE IF EXISTS dim_departamento
DROP TABLE IF EXISTS dim_product
DROP TABLE IF EXISTS dim_vendor
DROP TABLE IF EXISTS dim_time


-- Creating the dim_client table
CREATE TABLE dim_client (
    Codigo_cliente NVARCHAR(255) PRIMARY KEY,
    Nombre_cliente NVARCHAR(255),
    Volumen_cliente NVARCHAR(255),
    Frecuencia_cliente NVARCHAR(255),
	Primera_compra DATE,
	ULtima_compra DATE
);

CREATE TABLE dim_vendor (
    Codigo_vendedor INT PRIMARY KEY,
    Nombre_vendedor NVARCHAR(255)
);

CREATE TABLE dim_departamento (
    Codigo_departamento INT PRIMARY KEY,
    Departamento NVARCHAR(255)
);

CREATE TABLE dim_product (
    Codigo_producto NVARCHAR(255)  PRIMARY KEY,
    Nombre_producto NVARCHAR(255),
    Linea_producto NVARCHAR(255),
    Tipo_producto NVARCHAR(255)
);
CREATE TABLE dim_time (
    Fecha DATE  PRIMARY KEY,
    Dia_semana NVARCHAR(255),
    Semana INT,
    Mes NVARCHAR(255),
    Numero_mes INT,
    Ano INT
);

CREATE TABLE fact_sells(
    Codigo_venta BIGINT  NOT NULL,
    Fecha DATE ,
    Codigo_proyecto NVARCHAR(255),
    Cantidad_vendida FLOAT,
	Precio_unitario FLOAT,
    Precio_total FLOAT,
    Total_descuento FLOAT,
    Total_facturado FLOAT,
    Total_venta FLOAT,
    Total_contado INT,
    Total_credito FLOAT,
    Codigo_departamento INT,
    Codigo_cliente NVARCHAR(255) NOT NULL,
    Codigo_vendedor INT NOT NULL,
    Codigo_producto NVARCHAR(255) NOT NULL,
	PRIMARY KEY (Codigo_venta, Codigo_producto),
	FOREIGN KEY (Codigo_departamento) REFERENCES dim_departamento(Codigo_departamento),
	FOREIGN KEY (Codigo_cliente) REFERENCES dim_client(Codigo_cliente),
    FOREIGN KEY (Codigo_vendedor) REFERENCES dim_vendor(Codigo_vendedor),
    FOREIGN KEY (Codigo_producto) REFERENCES dim_product(Codigo_producto),
	FOREIGN KEY (Fecha) REFERENCES dim_time(Fecha),
);

CREATE TABLE fact_objective (
    Codigo_objetivo INT IDENTITY(1,1) PRIMARY KEY,
	Codigo_departamento INT,
	Fecha DATE, 
	Mes NVARCHAR(250),
	Objetivo INT,
	Valor_minimo INT,
	Valor_maximo INT,
	FOREIGN KEY (Codigo_departamento) REFERENCES dim_departamento(Codigo_departamento),
	FOREIGN KEY (Fecha) REFERENCES dim_time(Fecha),
);
