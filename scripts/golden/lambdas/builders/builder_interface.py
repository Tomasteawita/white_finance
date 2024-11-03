from sqlalchemy import create_engine
from os import environ
import boto3

class BuilderInterface():

    postgres_password = environ['POSTGRES_PASSWORD']
    postgres_user = environ['POSTGRES_USER']
    postgres_host = environ['POSTGRES_HOST']
    postgres_port = environ['POSTGRES_PORT']
    postgres_database = environ['POSTGRES_DATABASE']

    def __init__(self):
        print('Creating engine')
        self.engine = self._set_engine()
        print('Engine created')

    def _set_engine(self):
        print('Creating engine')
        return create_engine(
            f'postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}'
        )
    def load_data(self, df, schema, table_name):
        """Cargo los datos del dataframe en la tabla"""
        df.to_sql(table_name, self.engine, schema=schema, if_exists='append', index=False)
        print(f'Data loaded in {table_name}')

    def build_tables(self):
        pass

    def extract_data(self, source_bucket, source_key):
        """Leo un archivo de s3 y lo devuelvo"""
        print('Extracting data from s3')
        s3 = boto3.client('s3')
        print('s3 client created')
        print(f'Getting object from {source_bucket} and {source_key}')
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        print('Object obtained')
        return obj['Body'].read().decode('utf-8')
