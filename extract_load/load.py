import os
import zipfile
import pandas as pd
import database as db
from utils import get_config

def list_zip_files(directory):
    """
    Lista todos os arquivos .zip no diretório fornecido.
    """
    return [f for f in os.listdir(directory) if f.endswith('.zip')]

def extract_csv_from_zip(zip_path, extract_to='raw\\'):
    """
    Extrai arquivos .csv de um arquivo .zip para um diretório temporário.
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

def ingestion_process(zip_files_path, db_connction):
    zip_files = list_zip_files(zip_files_path)
    
    # Lista todos os arquivos .zip no diretório
    for zip_file in zip_files:
        zip_path = os.path.join(zip_files_path, zip_file)
        
        #Extrai arquivos .csv do arquivo .zip
        print('Extracting CSV files from {}...'.format(zip_path))
        csv_files = extract_csv_from_zip(zip_path)
        print('CSV files extracted: {}'.format(csv_files))

        for csv_file in csv_files:
            # table = csv_file.split('/')[1].split('\\')[0]
            table = csv_file.split('\\')[1]

            print('Ingesting table {}...'.format(table))
            print('CSV file: {}'.format(csv_file))
            
            header = get_config(section='header_files').get(table).split(',')
            # header = [x.strip() for x in header]

            flg_success = db.append_data(
                client = db_connction
                , table = table
                , data = pd.read_csv(csv_file, sep=';', encoding="latin-1", 
                    low_memory=False, names=header
                )
            )
            if flg_success:
                print('Table {} successfully ingested.'.format(table))
            else:
                print('Table {} ingestion failed.'.format(table))
            print('-------------------------------------------')
            #     # Remove o arquivo .csv temporário
            #     os.remove(csv_file)
def main():
    credentials = get_config(section='supabase_db')
    db_connction = db.create_connection(credentials)

    # Diretório contendo os arquivos .zip
    zip_files_path = 'D:\EL DATAGOV CNAE'
    ingestion_process(zip_files_path, db_connction)
    
if __name__ == '__main__':
    main()
