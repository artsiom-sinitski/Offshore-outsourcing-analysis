"""
Purpose: Retrieve the data from the Central Data Storage, create and populate
the datamart tables ('star' schema), and then store it in Data Mart.  
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date: 07/02/2020
"""

from postgres_connector import PostgresConnector
from pyspark.sql import SparkSession
from datetime import datetime
#import time
import sys
import logging


class GenerateAndSaveDataToDatamart():

    def __init__(self, run_type, timestamp):
        """
        Class constructor that initializes created objects with the default values
        """
        log_fname = "../logs/generate_datamart_data.log" + timestamp
        logging.basicConfig(filename=log_fname, filemode='w', level=logging.INFO)
        # self.date_from = date_from
        # self.date_to = date_to
        
        self.spark = SparkSession \
                    .builder \
                    .appName('generate-data-for-datamart') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()


    #TODO:
    # (1) - connect via postgres connector class
    # (3) - 
    def read_data_from_central_storage(self):
        database = "postgres"   #get_env_var("POSTGRES_DB")
        table = "gdelt_events"
        host = "10.0.0.04"      #get_env_var("POSTGRES_HOST")
        port = "5432"
        jdbcUrl = "jdbc:postgresql://{host}:{port}/{db}".format(host=host, \
                                                                 port=port, \
                                                                 db=database)
        query = "(select * from gdelt_events limit 5) ge"

        properties = { "user"     : "postgres", 
                       "password" : "",
                       "driver"   : "org.postgresql.Driver"
                     }

        # connector = PostgresConnector()
        # connector.write_to_db(data_frame, db_table, self.mode)

        df = self.spark.read \
                       .jdbc(url=jdbcUrl, table=query, properties=properties)

        #df.printSchema()
        df.show(3, True)
        return df


    #TODO
    # (1) - add aliases to the column names
    # (2) - add the rest of the star schema tables
    def populate_datamart_tables(self, df):
        cols = ['IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 'SourceUrl']
        event_dim_df = df.select(*cols)

        event_dim_df.printSchema()
        event_dim_df.show(5, True)


    #TODO
    def save_tables_to_datamart(self):
        database = "Data_Mart_0"
        host = "10.0.0.04"
        port = "5432"
        jdbcUrl = "jdbc:postgresql://{host}:{port}/{db}".format(host=host, \
                                                                port=port, \
                                                                db=database)



    #TODO
    def run(self):
        in_df = self.read_data_from_central_storage()
        out_df = self.populate_datamart_tables(in_df)

################## End Of GenerateAndSaveDataToDatamart class ################

def main():
    if len(sys.argv) == 2 and \
       sys.argv[1] in ["manual", "schedule"]:
       
        run_type = sys.argv[1]
        datetime_now = datetime.now()
        date_time = datetime_now.strftime("%Y-%m-%d %H:%M:%S.%f")
        timestamp = datetime_now.strftime("%d%m%Y%H%M%S")

        process = GenerateAndSaveDataToDatamart(run_type, timestamp)

        print("Date: " + date_time)
        logging.info("Date: " + date_time)

        print("Execution mode: " + run_type)
        logging.info("Execution mode: " + run_type)

        print("\n========== Started reading data from DW into Spark! ==========\n")
        logging.info("\n========== Started reading data from DW into Spark! ==========\n")

        process.run()

        print("\n========== Finished reading data from DW into Spark! ==========\n")
        logging.info("\n========== Finished reading data from DW into Spark! ==========\n")
    else:
        sys.stderr.write("Correct usage: python3 generate_save_to_datamart.py [schedule | manual]\n")
        logging.warning("Correct usage: python3 generate_save_to_datamart.py [schedule | manual]\n")


if __name__ == '__main__':
    main()
