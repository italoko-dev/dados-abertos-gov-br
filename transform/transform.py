import duckdb
from utils import get_config

def read_sql_file(sql_file):
    with open(sql_file, 'r', encoding="utf8") as file:
        sql_transform = file.read()
    return sql_transform

def execute_query_duckdb(sql_transform, db_file='file.db',
    db_name = 'dados_abertos_gov_br', create_schema=None):
    
    with duckdb.connect(db_file) as con:
        if db_name:
            con.execute('use {}'.format(db_name))
            print('\nUsing database: {}'.format(db_name))
        if create_schema:
            con.execute('create schema if not exists {}'.format(create_schema))
            print('\nCreating schema: {}'.format(create_schema))
        
        print('Transforming data...\n')
        print('-------------------------------------------')
        print(sql_transform)
        print('-------------------------------------------')
        try:
            con.sql(sql_transform)
            print('Success')
            print('-------------------------------------------')
        except Exception as e:
            print(e)

def main():
    credentials= get_config(filename='credentials.ini', section='motherduck')
    db_file='md:?motherduck_token='+credentials.get('md_token')
    db_name = credentials.get('db_name')
    schema = credentials.get('mart_receita_federal')
    
    sql_transform=read_sql_file('transform/estabelecimentos_cnaes_tb.sql')
    execute_query_duckdb(sql_transform, db_file=db_file, db_name=db_name
        , create_schema=schema)
if __name__ == '__main__':
    main() 
