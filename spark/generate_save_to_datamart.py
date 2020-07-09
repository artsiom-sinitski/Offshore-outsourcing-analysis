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

        self.dm_cols_dict = dict()
        self.dm_row_id_names_dict = dict()
        self.initialize_dm_columns_dictionary()
        
        self.spark = SparkSession \
                    .builder \
                    .appName('generate-data-for-datamart') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()


    #TODO: add columns for all the tables
    def initialize_dm_columns_dictionary(self):
        """[summary]
        """
        dm_table_name = "Event_Fact"
        self.dm_cols_dict[dm_table_name] = ["GoldsteinScale", "NumMentions", "NumSources", \
                                           "NumArticles", "AvgTone", "SqlDate", \
                                           "DateAdded", "GlobalEventId"]
        
        dm_table_name = "Event_Dim"
        self.dm_cols_dict["Event_Dim"] = ["IsRootEvent", "EventCode", "EventBaseCode", \
                                          "EventRootCode", "QuadClass", "SourceUrl"]
        self.dm_row_id_names_dict[dm_table_name] = "event_id"

        dm_table_name = "Actor1_Dim"
        self.dm_cols_dict[dm_table_name] = ["Actor1Code", "Actor1Name", "Actor1CountryCode", \
                                           "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code", \
                                           "Actor1Religion2Code", "Actor1Type1Code"]
        self.dm_row_id_names_dict[dm_table_name] = "actor1_id"

        #dm_table_name = "Actor2_Dim"
        # self.dm_cols_dict["Actor2_Dim"] = ["Actor2Code", "Actor2Name", "Actor2CountryCode", \
        #                                     "Actor2KnownGroupCode", "Actor2EthnicCode", "Actor2Religion1Code", \
        #                                     "Actor2Religion2Code", "Actor2Type1Code"]
        #self.dm_row_id_names_dict[dm_table_name] = "actor2_id"

        #dm_table_name = "Actor1Geo_Dim"


    #TODO:
    # (1) - need to read data by a date period and save it to a parquet file
    # select count group by date to determine the size of the date slice
    # (2) pass query from the outside
    def read_data_from_central_storage(self):
        query = "(select * from gdelt_events where \"EventRootCode\" like '14%' limit 3) ge"

        connector = PostgresConnector(EndPoint.CENTRAL_STORAGE.value)
        df = connector.read_from_db(self.spark, query)

        # df.printSchema()
        df.show(3, True)
        return df


    #TODO:
    # (2) - add the rest of the star schema tables
    # (1) - base id should be the max row_id from the Event_Fact table
    def populate_datamart_tables(self, df):
        dm_tables_dict = dict()
        #generate a pseudorandom base id -> random int from (1, 9) range || seconds
        base_id = datetime.now().strftime(str(randrange(1,9)) + "%S.%f").replace('.', '')[:-2]
        unique_id = base_id + F.monotonically_increasing_id()
        row_id = F.lit(unique_id)

        # loop through the dict that holds columns for appropriate tables
        for table in self.dm_cols_dict.keys():
            cols = self.dm_cols_dict[table]
            if table == "Event_Fact":
                tmp_df = df.select(*cols).withColumn("event_id", row_id) \
                                         .withColumn("actor1_id", row_id) \
                                         .withColumn("actor2_id", F.lit("null")) \
                                         .withColumn("actor1geo_id", F.lit("null")) \
                                         .withColumn("actor2geo_id", F.lit("null")) \
                                         .withColumn("actiongeo_id", F.lit("null"))
            else:
                tmp_df = df.select(*cols).withColumn(self.dm_row_id_names_dict[table], row_id)
            
            dm_tables_dict[table] = tmp_df

            print("\n*** " + table + " DataFrame count: ", tmp_df.count(), end='\n')
            tmp_df.show(5, True)

        # ==================== Event_Dim table ====================================
        # ========================================================================= 
        # cols = ["IsRootEvent", "EventCode", "EventBaseCode", \
        #         "EventRootCode", "QuadClass", "SourceUrl"]
        # event_id = F.lit(unique_id)
        # event_dim_df = df.select(*cols).withColumn("event_id", event_id)

        # #event_dim_df.printSchema()
        # #event_dim_df.explain()
        # print("\n=========================================================\n")
        # print("\n*** Event_Dim DataFrame count: ", event_dim_df.count())
        # print("\n=========================================================\n")
        # event_dim_df.show(5, True)

        # dm_tables_dict["Event_Dim"] = event_dim_df

        # # ==================== Actor1_Dim table =================================
        # # =======================================================================
        # cols = ["Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", \
        #         "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code"]
        # actor1_id = F.lit(unique_id)
        # actor1_dim_df = df.select(*cols).withColumn("actor1_id", actor1_id)

        # #event_dim_df.printSchema()
        # print("\n=========================================================\n")
        # print("\n*** Actor1_Dim DataFrame count: ", actor1_dim_df.count())
        # print("\n=========================================================\n")
        # actor1_dim_df.show(5, True)

        # dm_tables_dict["Actor1_Dim"] = actor1_dim_df

        # # ==================== Event_Fact table =================================
        # # =======================================================================
        # cols = ["GoldsteinScale", "NumMentions", "NumSources", "NumArticles", \
        #         "AvgTone", "SqlDate", "DateAdded", "GlobalEventId"]
        # event_fact_df = df.select(*cols).withColumn("event_id", event_id) \
        #                                 .withColumn("actor1_id", actor1_id) \
        #                                 .withColumn("actor2_id", F.lit("null")) \
        #                                 .withColumn("actor1geo_id", F.lit("null")) \
        #                                 .withColumn("actor2geo_id", F.lit("null")) \
        #                                 .withColumn("actiongeo_id", F.lit("null"))

        # #event_dim_df.printSchema()
        # print("\n=========================================================\n")
        # print("\n*** Event_Fact DataFrame count: ", event_fact_df.count())
        # print("\n=========================================================\n")
        # event_fact_df.show(5, True)

        # dm_tables_dict["Event_Fact"] = event_fact_df

        return dm_tables_dict


    def save_tables_to_datamart(self, dm_tables_dict):
        """[summary]

        Args:
            dm_tables_dict ([type]): [description]
        """
        connector = PostgresConnector(EndPoint.PROTESTS_DATAMART.value)

        for table in dm_tables_dict.keys():
            df = dm_tables_dict[table]
            connector.write_to_db(df, table, self.mode)

        print("\n***** Saved dataframe to Datamart! *****\n")


    #TODO
    #(1) - loop through the data based on the date range
    def run(self):
        dw_df = self.read_data_from_central_storage()
        dm_tables_dict = self.populate_datamart_tables(dw_df)
        #print("\n****** DataFrames Dict content: ", dm_tables_dict)
        self.save_tables_to_datamart(dm_tables_dict)

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
