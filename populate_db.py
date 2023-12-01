import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sqlalchemy

# Replace the following variables with your specific server information
server_name = 'VELOZ'  # 
database_name = 'PlastiForte'
user_name = 'python_user'
password = '12345678'

final_data_path = "./documents/final_document.csv"
objective_data_path = "./documents/objective_document.csv"



conn_str = f'mssql+pyodbc://{user_name}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server'

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Excel {file_path} read correctly")
        return df
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return None
    except pd.errors.ParserError:
        print("Error while parsing the file.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo: {str(e)}")
    
# Read the document
try:
    df = read_csv_file(final_data_path)
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    print(f"Excel {final_data_path} ha sido correctamente, dimensiones {df.shape}" )

    # Create diferents df for the tables
        # dim Clients
    dim_client_df = df[["Codigo_cliente","Nombre_cliente","Volumen_cliente","Frecuencia_cliente","Fecha"]]
    dim_client_df = dim_client_df.drop_duplicates()
    # Los agrupoas por codigo de cliente y concat el nombre
    grouped_client_df = dim_client_df.groupby(['Codigo_cliente']).agg(
        Volumen_cliente=pd.NamedAgg(column='Volumen_cliente', aggfunc='first'),
        Frecuencia_cliente=pd.NamedAgg(column='Frecuencia_cliente', aggfunc='first'),
        Nombre_cliente=pd.NamedAgg(column='Nombre_cliente', aggfunc='first'),
        Primera_compra=pd.NamedAgg(column='Fecha', aggfunc='min'),
        ULtima_compra=pd.NamedAgg(column='Fecha', aggfunc='max')
    ).reset_index()
    grouped_client_df= grouped_client_df.drop_duplicates()
    print(f"dim_client_df: {grouped_client_df.shape}")
        #dim_vendor#
    dim_vendor_df = df[["Codigo_vendedor", "Nombre_vendedor"]]
    dim_vendor_df = dim_vendor_df.drop_duplicates()
    print(f"dim_vendor: {dim_vendor_df.shape}")    
        #dim_departamento#
    dim_departament_df = df[["Codigo_departamento", "Departamento"]]
    dim_departament_df = dim_departament_df.drop_duplicates()
    print(f"dim_departamento: {dim_departament_df.shape}") 
        #dim_product#
    dim_product_df = df[["Codigo_producto","Nombre_producto","Linea_producto","Tipo_producto"]]
    dim_product_df = dim_product_df.drop_duplicates()
    print(f"dim_product: {dim_product_df.shape}") 
        #dim_project#
    dim_project_df = df[["Codigo_proyecto", "Ubicacion_proyecto","Total_venta"]]
    grouped_project_df = dim_project_df.groupby(['Codigo_proyecto']).agg(
        Total_venta=pd.NamedAgg(column="Total_venta", aggfunc='sum'),
        Ubicacion_proyecto=pd.NamedAgg(column='Ubicacion_proyecto', aggfunc=lambda x: x.mode().iloc[0])
    ).reset_index()
    print(f"dim_project: {grouped_project_df.shape}") 
        #dim_time#
    dim_time_df  = df[["Fecha","Dia_semana","Semana", "Mes","Numero_mes", "Ano","Trimestre"]]
    dim_time_df = dim_time_df.drop_duplicates()
        #Fact_sells
    print(f"dim_time: {dim_time_df.shape}") 
    fact_sells_df = df[["Codigo_venta","Fecha", "Cantidad_vendida","Precio_unitario", "Precio_total", "Total_descuento", "Total_facturado","Total_venta",
                     "Total_contado","Total_credito","Es_proyecto","Codigo_proyecto","Codigo_departamento","Codigo_cliente","Codigo_vendedor","Codigo_producto"]]
    fact_sells_df = fact_sells_df.groupby(["Codigo_venta","Fecha","Codigo_proyecto","Codigo_departamento","Codigo_cliente","Codigo_vendedor","Codigo_producto","Es_proyecto"]).agg({
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
   
    print("Se crearon los datafreames correctamente")
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


def create_or_populate_table(engine, data_frame, table_name):
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
    
def get_and_tranform_objective_table(goals_data_path):
    df = read_csv_file(goals_data_path)
    fact_goal = df[["Codigo_departamento","Fecha","Mes","Objetivo","Valor_minimo","Valor_maximo"]]
    return fact_goal

fact_objective_df = get_and_tranform_objective_table(objective_data_path)

create_or_populate_table(engine,grouped_client_df,"dim_client")
create_or_populate_table(engine,dim_vendor_df,"dim_vendor")
create_or_populate_table(engine,dim_departament_df,"dim_departament")
create_or_populate_table(engine,dim_product_df,"dim_product")
create_or_populate_table(engine,grouped_project_df,"dim_project")
create_or_populate_table(engine,dim_time_df,"dim_time")
create_or_populate_table(engine,fact_sells_df,"fact_sells")
create_or_populate_table(engine,fact_objective_df,"fact_objective")



engine.dispose()
print(f"Se desconecto exitosamente con las base de datos: {database_name}")


