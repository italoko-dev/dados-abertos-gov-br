import duckdb
from utils import get_config

# def execute_query_duckdb(sql_transform, db_file='file.db',
#     db_name = 'dados_abertos_gov_br', db_target_schema=None):
    
#     with duckdb.connect(db_file) as con:
#         if db_name:
#             con.execute('use {}'.format(db_name))
#             print('\nUsing database: {}'.format(db_name))
#         print('Appending data to DuckDB...\n')
#         for parquet_path in dir_parquet_path:
#             table_name = parquet_path.split('\\')[1]
#             parquet_path += '\\*.parquet'

#             if db_target_schema:
#                 table_name = db_target_schema+'.'+table_name
#             print('Append data to table: {}'.format(table_name))

#             q = f"""
#                 create or replace table {table_name} as
#                 select * from read_parquet('{parquet_path}')
#             """
#             try:
#                 con.sql(q)
#                 print('Success')
#                 print('-------------------------------------------')
#             except Exception as e:
#                 print(e)

def main():
    zip_files_path = get_config(section='path_raw_files').get('receita_federal')
    
    credentials= get_config(filename='credentials.ini', section='motherduck')
    db_file='md:?motherduck_token='+credentials.get('md_token')
    db_name = credentials.get('db_name')
    schema = get_config(filename='credentials.ini'
                        , section='schema_receita_federal').get('schema')
    
    print(credentials)    
if __name__ == '__main__':
    main() 
