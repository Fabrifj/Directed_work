import pandas as pd

def main():
    source_documents = ["./documents/ventas_vendedor_2016-2017.xls","./documents/ventas_vendedor_2016-2017-2.xls",
                        "./documents/ventas_vendedor_2017-2018.xls","./documents/ventas_vendedor_2017-2018-2.xls",
                        "./documents/ventas_vendedor_2018-2019.xls","./documents/ventas_vendedor_2018-2019-2.xls",
                        "./documents/ventas_vendedor_2019-2020.xls","./documents/ventas_vendedor_2019-2020-2.xls",
                        "./documents/ventas_vendedor_2020-2021.xls","./documents/ventas_vendedor_2020-2021-2.xls",
                        "./documents/ventas_vendedor_2021-2022.xls","./documents/ventas_vendedor_2021-2022-2.xls"]
    transformed_document = "total_sells.csv"
    data_frames = []
    try:
        for document in source_documents:
            df = read_excel_file(document)
            data_frames.append(clean_and_transform_data(df))
        final_df = pd.concat(data_frames, ignore_index=True)
        final_df = final_df.drop_duplicates()
        save_transformed_data(final_df, transformed_document)
    except FileNotFoundError:
        print(f"The file '{document}' was not found.")
    except pd.errors.ParserError:
        print("Error while parsing the file.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def read_excel_file(file_path):
    try:
        df = pd.read_excel(file_path)
        print(f"Excel {file_path} read correctly")
        return df
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return None
    except pd.errors.ParserError:
        print("Error while parsing the file.")
        return None

def clean_and_transform_data(df):
    if df is None:
        return
    
    try:
        # Clean headers and empty rows
        df_clean = clean_and_trim_dataframe(df)
        # Detect sells and products, then transform
        df_clean = combine_sales_and_products(df_clean)
        # Decorate dataframe
        df_clean = decorate_data(df_clean)
        print("Data Frame trasformed & clean correctly ")
        return df_clean
    except KeyError:
        print("A required column is missing in the DataFrame.")
    except ValueError:
        print("Error while converting values to integers.")
    except Exception as e:
        print(f"An error occurred while cleaning and transforming the data: {str(e)}")
def clean_and_trim_dataframe(df):
    # Trim the first 15 rows
    df_trimmed = df.iloc[15:].reset_index(drop=True)
    
    # Remove rows where all elements are NaN
    df_trimmed.dropna(how='all', inplace=True)
    
    # Remove rows where the first column is NaN
    df_trimmed = df_trimmed[df_trimmed.iloc[:, 0].notna()]
    print("Data Frame trasformed correctly ")

    return df_trimmed

def combine_sales_and_products(df):
    # Your combine_sales_and_products function code here
    results = []
    actual_sell = None
    df["isProduct"] = df.iloc[:, 0].apply(lambda x:True if (len(str(x)) > 10 or str(x).startswith('ZT20') or str(x).startswith('ZT10')) else False)
    for indice, row in df.iterrows():
        if row.isProduct :  
            result_row = pd.concat([actual_sell, row], ignore_index=True,axis=0)
            results.append(result_row)
        else:
            actual_sell = row
            

    # Creamos un nuevo DataFrame con las filas combinadas
    df_combinado = pd.DataFrame(results, index=None)
    print("Data Frame combined correctly ")

    return df_combinado
def decorate_data(df):
    try:
        #Rename Columns
        nombres_columnas = {0:'Codigo_venta', 1:'Fecha',2:'None1',3:'Codigo_departamento',4:'Caja',5: 'T_C',6: 'Codigo_cliente',7: 'Nombre_cliente',8: 'Codigo_vendedor',9: 'Codigo_proyecto',
                    10:'None2', 11:'None3', 12:'None4',13:'None5',14:'Flag_venta', 15:'Codigo_producto',16:'None6', 17:'Nombre_producto',18:'None7', 19:'Linea_producto',20:'None8',21: 'Cantidad_vendida',
                    22:'Precio_total', 23:'Total_descuento', 24:'Total_facturado',25:'Total_impuesto', 26:'Total_venta', 27:'Total_contado',28:'Total_credito',29:'Flag_producto'
                    }
        df_rename = df.rename(columns=nombres_columnas)
        # Drop nan columns
        drop_columns = ['None1', 'None2','None3','None4','None5','None6','None7','None8','Flag_producto','Flag_venta']
        df_rename = df_rename.drop(columns=drop_columns)
        print("Data Frame decorate correctly ")
        return df_rename
    except Exception as e:
        print(f"An error occurred while saving the transformed data: {str(e)}")

def save_transformed_data(df, output_path):
    if df is not None:
        try:
            print(f"Data frame shape: {df.shape}")
            df.to_csv(output_path, index=False)
            print("Data Frame saved correctly ")

        except Exception as e:
            print(f"An error occurred while saving the transformed data: {str(e)}")

if __name__ == "__main__":
    main()
