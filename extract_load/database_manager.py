from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class PostgresDB:
    def __init__(self, user, password, host, port, database):
        """
        Initializes a new instance of the PostgresDB class.

        Parameters:
            user (str): The username for the PostgreSQL database.
            password (str): The password for the PostgreSQL database.
            host (str): The hostname or IP address of the PostgreSQL server.
            port (int): The port number of the PostgreSQL server.
            database (str): The name of the PostgreSQL database.

        Returns:
            Boolean: True if the connection was successful, False otherwise.
        """
        self.database_url = f"postgresql://{user}:{password}@{host}:{port}\
            /{database}"
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)

    def append_data(self, schema, table_name, dataframe):
        """
        Inserts data from a pandas DataFrame into a PostgreSQL table.

        Parameters:
            schema (str): The name of the database schema.
            table_name (str): The name of the table to insert the data into.
            dataframe (pandas.DataFrame): The DataFrame containing the data 
            to be inserted.

        Returns:
            bool: True if the data was successfully inserted, False otherwise.
        """
        try:
            dataframe.to_sql(table_name, self.engine,schema=schema,
                if_exists='append', index=False)
            print(f"Inserted {len(dataframe)} records into {table_name} table.")
            return True
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            return False