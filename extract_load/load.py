import os
import zipfile
import pandas as pd
from  database_manager import PostgresDB
from utils import get_config

def list_zip_files(directory):
    """
    Returns a list of all the zip files in the given directory.

    Parameters:
        directory (str): The path to the directory to search for zip files.

    Returns:
        list: A list of strings representing the names of all the 
        zip files in the directory.
    """
    return [f for f in os.listdir(directory) if f.endswith('.zip')]

def extract_csv_from_zip(zip_path, extract_to='raw\\'):
    """
    Extracts CSV files from a given ZIP file and saves them 
    to a specified directory.

    Parameters:
        zip_path (str): The path to the ZIP file.
        extract_to (str, optional): The directory to extract the CSV files to. 
        Defaults to 'raw\\'.

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

def ingestion_process(zip_files_path, db_connction, schema='public'):
    """
    Processes a list of zip files, extracts CSV files from each zip file, 
    and ingests the data into a database.

    Args:
        zip_files_path (str): The path to the directory containing the zip files
        db_connction (DatabaseConnection): The database connection object.
        schema (str, optional): The schema name in the database.
          Defaults to 'public'.

    Returns:
        None

    Raises:
        Exception: If the section is not found in the settings file.
    """
    zip_files = list_zip_files(zip_files_path)
    
    # List zip files in directory
    for zip_file in zip_files:
        zip_path = os.path.join(zip_files_path, zip_file)
        
        #Extract csv files of the zip file
        print('Extracting CSV files from {}...'.format(zip_path))
        csv_files = extract_csv_from_zip(zip_path)
        print('CSV files extracted: {}'.format(csv_files))

        for csv_file in csv_files:
            table = csv_file.split('\\')[1]

            print('Ingesting table {}...'.format(table))
            print('CSV file: {}'.format(csv_file))
            
            header = get_config(section='header_files').get(table).split(',')
            header = [x.strip() for x in header]

            df = pd.read_csv(csv_file, sep=';', encoding="latin-1", 
                             low_memory=False, names=header)
            
            flg_success = db_connction.append_data(
                schema = schema
                , table_name = table
                , dataframe = df
            )
            if flg_success:
                print('Table {} successfully ingested.'.format(table))
            else:
                print('Table {} ingestion failed.'.format(table))
            print('-------------------------------------------')

def main():

    credentials = get_config(filename='credentials.ini', section='postgres')
    schema = get_config(filename='credentials.ini'
                        , section='schema_receita_federal').get('schema')
    db_connction = PostgresDB(
        user= credentials.get('user')
        , password= credentials.get('password')
        , host=credentials.get('host')
        , port=credentials.get('port')
        , database=credentials.get('db_name')
    )

    zip_files_path = get_config(section='path_raw_files').get('receita_federal')
    
    ingestion_process(zip_files_path, db_connction, schema=schema)
    
if __name__ == '__main__':
    main()
