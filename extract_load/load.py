import os
import zipfile
import pandas as pd
import duckdb
from utils import get_config

def extract_csv_from_zip(zip_path, extract_to):
    """
    Extracts CSV files from a given ZIP file and saves them to a specified directory.
    Parameters:
        zip_path (str): The path to the ZIP file.
        extract_to (str): The directory to extract the CSV files to.
    Returns:
        list: A list of paths to the extracted CSV files.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_name = zip_ref.namelist()[0]

        if file_name.endswith('.EMPRECSV'):
            extract_to+='empresas'
        elif file_name.endswith('.ESTABELE'):
            extract_to+='estabelecimentos'
        elif file_name.endswith('.SIMPLES.CSV.D40413'):
            extract_to+='dados_do_simples'
        elif file_name.endswith('.SOCIOCSV'):
                extract_to+='socios'
        elif file_name.endswith('.PAISCSV'):
            extract_to+='paises'
        elif file_name.endswith('.MUNICCSV'):
            extract_to+='municipios'
        elif file_name.endswith('.MOTICSV'):
            extract_to+='motivo_situacao_cadastral'
        elif file_name.endswith('.QUALSCSV'):
            extract_to+='qualificacao_socio'
        elif file_name.endswith('.NATJUCSV'):
            extract_to+='natureza_juridica'
        elif file_name.endswith('.CNAECSV'):
            extract_to+='cnaes'

        zip_ref.extractall(extract_to)
        return [os.path.join(extract_to, f) for f in zip_ref.namelist() ]

def convert_zip_files_to_parquet(zip_files_path):
    """
    Converts zip files in the specified directory to Parquet format.

    Args:
        zip_files_path (str): The path to the directory containing the zip files.

    Returns:
        list: A list of paths to the Parquet files created. Returns None if an exception occurs.

    Prints:
        - 'Converting files to Parquet...'
        - 'CSV file: {csv_file}' for each CSV file being converted
        - 'Converted Parquet file: {csv_file}.parquet' for each CSV file converted to Parquet
        - 'Parquet files converted successfully!' if all files are successfully converted

    Raises:
        Exception: If an error occurs during the conversion process.
    """
    print('\n-------------------------------------------')
    extract_to = 'raw\\'
    list_parquet_path = list()
    zip_files = [f for f in os.listdir(zip_files_path) if f.endswith('.zip')]
    try:
    # List zip files in directory
        for zip_file in zip_files:
            zip_path = os.path.join(zip_files_path, zip_file)

            csv_files = extract_csv_from_zip(zip_path, 'raw\\')
            
            for csv_file in csv_files:

                table = csv_file.split('\\')[1]
                
                header = get_config(section='header_files').get(table).split(',')
                header = [x.strip() for x in header]
            
                print('CSV file: {}'.format(csv_file))
                
                pd.read_csv(csv_file, sep=';', encoding="latin-1", 
                    low_memory=False, names=header).to_parquet(csv_file+'.parquet')
                
                print('Converted Parquet file: {}'.format(csv_file+'.parquet'))
                
                parquet_path = extract_to + table
                if parquet_path not in list_parquet_path:
                    list_parquet_path.append(parquet_path)
                
                os.remove(csv_file)
                
                print('-------------------------------------------')
        print('Parquet files converted successfully!')
        return list_parquet_path
    except Exception as e:
        print(e)
        return None

def append_data_duckdb(dir_parquet_path, db_file='file.db',
    db_name = 'dados_abertos_gov_br', db_schema=None):
    """
    Appends data from Parquet files in the specified directory to a DuckDB database.

    Args:
        dir_parquet_path (str): The path to the directory containing the Parquet files.
        db_file (str, optional): The path to the DuckDB database file. Defaults to 'file.db'.
        db_name (str, optional): The name of the database in the DuckDB database file. Defaults to 'dados_abertos_gov_br'.
        db_schema (str, optional): The schema name in the DuckDB database. Defaults to None.

    Returns:
        None

    Prints:
        - 'Using database: {db_name}' if db_name is provided
        - 'Appending data to DuckDB...'
        - 'Append data to table: {table_name}' for each table being appended
        - 'Success' for each successful append
        - The error message if an exception occurs during the append process

    Raises:
        Exception: If an error occurs during the append process.
    """
    
    with duckdb.connect(db_file) as con:
        if db_name:
            con.execute('use {}'.format(db_name))
            print('\nUsing database: {}'.format(db_name))
        print('Appending data to DuckDB...\n')
        for parquet_path in dir_parquet_path:
            table_name = parquet_path.split('\\')[1]
            parquet_path += '\\*.parquet'

            if db_schema:
                table_name = db_schema+'.'+table_name
            print('Append data to table: {}'.format(table_name))

            q = f"""
                create or replace table {table_name} as
                select * from read_parquet('{parquet_path}')
            """
            try:
                con.sql(q)
                print('Success')
                print('-------------------------------------------')
            except Exception as e:
                print(e)
            
def ingestion_process(zip_files_path, db_file, db_name, schema):
    """
    Ingests data from zip files into a DuckDB database.

    Args:
        zip_files_path (str): The path to the directory containing the zip files.
        db_file (str): The path to the DuckDB database file.
        db_name (str): The name of the database in the DuckDB database file.
        schema (str): The schema name in the DuckDB database.

    Returns:
        None

    This function performs the following steps:
    1. Converts zip files in the specified directory to Parquet format using the `convert_zip_files_to_parquet` function.
    2. Appends the Parquet data to the DuckDB database using the `append_data_duckdb` function.

    Note:
    - The `convert_zip_files_to_parquet` function is responsible for converting zip files to Parquet format.
    - The `append_data_duckdb` function is responsible for appending the Parquet data to the DuckDB database.
    """
    
    list_parquet_path = convert_zip_files_to_parquet(zip_files_path)
    
    append_data_duckdb(list_parquet_path
        , db_file=db_file
        , db_name=db_name
        , db_schema=schema)
    
def main():
    zip_files_path = get_config(section='path_raw_files').get('receita_federal')
    
    credentials= get_config(filename='credentials.ini', section='motherduck')
    db_file='md:?motherduck_token='+credentials.get('md_token')
    db_name = credentials.get('db_name')
    schema = get_config(filename='credentials.ini'
                        , section='schema_receita_federal').get('schema')
    
    ingestion_process(zip_files_path, db_file, db_name, schema)
        
if __name__ == '__main__':
    main() 
