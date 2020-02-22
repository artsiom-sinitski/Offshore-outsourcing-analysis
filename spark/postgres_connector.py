import os
import logging
from pyspark.sql import DataFrameWriter


class PostgresConnector(object):
    def __init__(self):
        self.database = get_env_var("POSTGRES_DB")
        self.host = get_env_var("POSTGRES_HOST")
        self.port = '5432'
        self.url = 'jdbc:postgresql://{host}:{port}/{db}'.format(host=self.host,\
                                                                 port=self.port,\
                                                                   db=self.database)
        self.properties = { "user"     : get_env_var("POSTGRES_USR"), 
                            "password" : get_env_var("POSTGRES_PWD"),
                            "driver"   : "org.postgresql.Driver"
                          }


    def get_writer(self, df):
        return DataFrameWriter(df)


    def write_to_db(self, df, table, mode):
        writer = self.get_writer(df)
        writer.jdbc(self.url, table, mode, self.properties)


def get_env_var(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Environment variable '{}' not set.".format(name)
        raise Exception(message)
