import logging
# import psycopg2
from pyspark.sql import DataFrameWriter

class PostgresConnector(object):
    def __init__(self):
        self.database = 'postgres'
        self.username = 'postgres'
        self.password = ''
        self.host = '10.0.0.14'
        self.port = '5432'
        self.url = 'jdbc:postgresql://{host}:{port}/{db}'.format(host=self.host, port=self.port, db=self.database)
        self.properties = { "user"     : self.username, 
                            "password" : self.password,
                            "driver"   : "org.postgresql.Driver"
                          }


    def get_writer(self, df):
        return DataFrameWriter(df)


    def write_to_db(self, df, table, mode):
        writer = self.get_writer(df)
        writer.jdbc(self.url, table, mode, self.properties)

