import os
import zipfile
import pandas as pd

def list_zip_files(directory):
    """
    Lista todos os arquivos .zip no diretório fornecido.
    """
    return [f for f in os.listdir(directory) if f.endswith('.zip')]

def extract_csv_from_zip(zip_path, extract_to='raw/'):
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
            extract_to+='naturezas_juridicas'
        elif file_name.endswith('.CNAECSV'):
            extract_to+='cnaes'

        zip_ref.extractall(extract_to)
        return [os.path.join(extract_to, f) for f in zip_ref.namelist() ]

def load_process(zip_files_path):
    zip_files = list_zip_files(zip_files_path)
    
    # Lista todos os arquivos .zip no diretório
    for zip_file in zip_files:
        zip_path = os.path.join(zip_files_path, zip_file)
        
        # Extrai arquivos .csv do arquivo .zip
        csv_files = extract_csv_from_zip(zip_path)
        
        for csv_file in csv_files:
            print(csv_file)
            #     # Remove o arquivo .csv temporário
            #     os.remove(csv_file)
def main():
    # Diretório contendo os arquivos .zip
    zip_files_path = 'D:\EL DATAGOV CNAE'
    load_process(zip_files_path)
    
if __name__ == '__main__':
    main()