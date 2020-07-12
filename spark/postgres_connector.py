"""
Purpose: To provide read/write interface to the PostgreSQL databases used by the Data Warehouse. 
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
"""
from constants import EndPoint

from pyspark.sql import DataFrameWriter
import logging
import os


class PostgresConnector(object):
    def __init__(self, end_point):
        if end_point == EndPoint.CENTRAL_STORAGE.value:
            self.host = get_env_var("POSTGRES_HOST")
            self.database = get_env_var("POSTGRES_DB")
        elif end_point == EndPoint.PROTESTS_DATAMART.value:
            self.host = get_env_var("DATAMART_HOST")
            self.database = get_env_var("DATAMART_DB")

        self.port = '5432'
        self.url = 'jdbc:postgresql://{host}:{port}/{db}'.format(host=self.host, \
                                                                 port=self.port, \
                                                                   db=self.database)
        self.properties = { "user"     : get_env_var("POSTGRES_USR"), 
                            "password" : get_env_var("POSTGRES_PWD"),
                            "driver"   : "org.postgresql.Driver"
                          }


    def read_from_db(self, spark, table):
        # 'spark' is the Spark session object
        reader = self.get_reader(spark)
        df = reader.jdbc(url=self.url, table=table, properties=self.properties)
        return df
    
    def get_reader(self, spark):
        return spark.read   # DataFrameReader(spark)


    def write_to_db(self, df, table, mode):
        writer = self.get_writer(df)
        writer.jdbc(self.url, table, mode, self.properties)

    def get_writer(self, df):
        return DataFrameWriter(df)


def get_env_var(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Environment variable '{}' not set.".format(name)
        raise Exception(message)
