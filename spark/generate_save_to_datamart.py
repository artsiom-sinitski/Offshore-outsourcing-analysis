"""
Purpose: Retrieve the data from the Central Data Storage, create and populate
the datamart tables ('star' schema), and then store it in the Protests DataMart.  
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date: 07/02/2020
"""
from postgres_connector import PostgresConnector
from constants import EndPoint

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from random import randrange
import logging
import sys


class GenerateAndSaveDataToDatamart(object):

    def __init__(self, run_type, timestamp):
        """
        Class constructor that initializes created objects with the default values
        """
        log_fname = "../logs/generate_datamart_data.log" + timestamp
        logging.basicConfig(filename=log_fname, filemode='w', level=logging.INFO)
        # self.date_from = date_from
        # self.date_to = date_to
        self.mode = "append"
        
        self.spark = SparkSession \
                    .builder \
                    .appName('generate-data-for-datamart') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()


    #TODO:
    # (1) - need to read data by a date period and save it to a parquet file
    # select count group by date to determine the size of the date slice
    def read_data_from_central_storage(self):
        query = "(select * from gdelt_events where \"EventRootCode\" like '14%' limit 17) ge"

        connector = PostgresConnector(EndPoint.CENTRAL_STORAGE.value)
        df = connector.read_from_db(self.spark, query)

        # The line below works as expected:
        #   df = self.spark.read.jdbc(url=url, table=query, properties=properties)
        # Also, it should work how it is written below but it doesn't:
        #   df = DataFrameReader(self.spark).jdbc(url=url, table=query, properties=properties)
        #see https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader

        # df.printSchema()
        df.show(3, True)
        return df


    #TODO
    # (2) - add the rest of the star schema tables
    def populate_datamart_tables(self, df):
        dm_table_dict = dict()
        #generate a pseudorandom base id -> random int from (1, 9) range || seconds
        base_id = datetime.now().strftime(str(randrange(1,9)) + "%S.%f").replace('.', '')[:-2]

        # =========== Event_Dim table ======================= 
        cols = ["IsRootEvent", "EventCode", "EventBaseCode", \
                "EventRootCode", "QuadClass", "SourceUrl"]
        event_dim_id = base_id + F.monotonically_increasing_id()
        #where_clause = "\"EventRootCode\" like '14%'"
        event_dim_df = df.select(*cols).withColumn("event_id", F.lit(event_dim_id))

        #event_dim_df.printSchema()
        print("\n=========================================================\n")
        #event_dim_df.explain()
        print("\n*** DataFrame count: ", event_dim_df.count())
        print("\n=========================================================\n")
        event_dim_df.show(5, True)

        dm_table_dict["Event_Dim"] = event_dim_df
        return dm_table_dict


    def save_tables_to_datamart(self, dm_table_dict):
        connector = PostgresConnector(EndPoint.PROTESTS_DATAMART.value)

        for table in dm_table_dict.keys():
            df = dm_table_dict[table]
            connector.write_to_db(df, table, self.mode)

        print("\n***** Saved dataframe to Datamart! *****\n")


    #TODO
    #(1) - loop through the data based on the date range
    def run(self):
        dw_df = self.read_data_from_central_storage()
        dm_table_dict = self.populate_datamart_tables(dw_df)
        #print("\n****** DataFrames Dict content: ", dm_table_dict)
        self.save_tables_to_datamart(dm_table_dict)

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
