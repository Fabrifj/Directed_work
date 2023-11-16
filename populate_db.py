import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sqlalchemy

# Replace the following variables with your specific server information
server_name = 'VELOZ'  # 
database_name = 'PlastiForte'
user_name = 'python_user'
password = '12345678'

final_data_path = "./final_document.csv"
conn_str = f'mssql+pyodbc://{user_name}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server'


# Read the document
try:
    df = pd.read_csv(final_data_path)
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    print(f"Excel {final_data_path} ha sido correctamente, dimensiones {df.shape}" )

    # Create diferents df for the tables
    fact_sells_df = df[["Codigo_venta","Fecha","Codigo_proyecto", "Cantidad_vendida","Precio_unitario", "Precio_total", "Total_descuento", "Total_facturado","Total_venta",
                     "Total_contado","Total_credito","Departamento","Codigo_cliente","Codigo_vendedor","Codigo_producto"]]
    fact_sells_df = fact_sells_df.groupby(["Codigo_venta","Fecha","Codigo_proyecto","Departamento","Codigo_cliente","Codigo_vendedor","Codigo_producto"]).agg({
    "Cantidad_vendida": "sum",
    "Precio_unitario": "mean",
    "Precio_total": "sum",
    "Total_descuento": "sum",
    "Total_facturado": "sum",
    "Total_venta": "sum",
    "Total_contado": "sum",
    "Total_credito": "sum",
    }).reset_index()
    print(f"fact_sells: {fact_sells_df.shape}")

    # dim Clients
    dim_client_df = df[["Codigo_cliente","Nombre_cliente","Volumen_cliente","Frecuencia_cliente"]]
    dim_client_df = dim_client_df.drop_duplicates()
    # Los agrupoas por codigo de cliente y concat el nombre
    grouped_client_df = dim_client_df.groupby(['Codigo_cliente']).agg({
        "Volumen_cliente": "first",
        "Frecuencia_cliente": "first",
        "Nombre_cliente": "first"
    }).reset_index()
    grouped_client_df["Codigo_cliente"] = grouped_client_df["Codigo_cliente"].where( grouped_client_df["Codigo_cliente"]!= "o", "o-o")
    print(f"dim_client_df: {grouped_client_df.shape}")
        
    dim_vendor_df = df[["Codigo_vendedor", "Nombre_vendedor"]]
    dim_vendor_df = dim_vendor_df.drop_duplicates()
    print(f"dim_vendor: {dim_vendor_df.shape}")    

    dim_product_df = df[["Codigo_producto","Nombre_producto","Linea_producto","Tipo_producto"]]
    dim_product_df = dim_product_df.drop_duplicates()
    print(f"dim_product: {dim_product_df.shape}") 

    dim_time_df  = df[["Fecha","Dia_Semana", "Mes", "Ano"]]
    dim_time_df = dim_time_df.drop_duplicates()
    print(f"dim_time: {dim_time_df.shape}") 

    print("Se crearon los datafreames correctamente")

except FileNotFoundError:
    print(f"El archivo '{final_data_path}' no se encontró.")
except pd.errors.EmptyDataError:
    print("File is empty. Please provide a file with data.")
except ValueError:
    print("Type conversion error. Check if all values in the column are convertible to the desired type.")
except KeyError:
    print("Column not found in DataFrame.")
except Exception as e:
    print(f"Ocurrió un error al leer el archivo: {str(e)}")


# # Create the connection string
engine = create_engine(conn_str)
# # Create an inspector object

#     # print(f"Se conecto exitosamente con las base de datos: {database}")

def check_table_exists_and_create(engine, data_frame, table_name):
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'"))
            table_exists = result.fetchone() is not None
            if_exists_option = 'append' if table_exists else 'replace'
            data_frame.to_sql(table_name, engine, if_exists=if_exists_option, index=False)
            print(f"Table {table_name} exists: {table_exists}")
            return table_exists
    except sqlalchemy.exc.IntegrityError as e:
        print(f"Integrity Error: {e}")
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"SQLAlchemy Error: {e}")
    except sqlalchemy.exc.DBAPIError as e:
        print(f"SQLAlchemy Error: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")
        return False

check_table_exists_and_create(engine,grouped_client_df,"dim_client")
check_table_exists_and_create(engine,dim_vendor_df,"dim_vendor")
check_table_exists_and_create(engine,dim_product_df,"dim_product")
check_table_exists_and_create(engine,dim_time_df,"dim_time")
check_table_exists_and_create(engine,fact_sells_df,"fact_sells")


engine.dispose()
print(f"Se desconecto exitosamente con las base de datos: {database_name}")


