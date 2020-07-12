"""
Purpose: Retrieve the apropriate data slice from the Central Data Storage,
create and populate the datamart tables (accroding to the defined 'star' schema),
and then store tables in the Protests DataMart database.  
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

    def __init__(self, run_type, timestamp, query):
        """Class constructor that initializes created objects with the default values
        Args:
            run_type (string):          possible values are "schedule" or "manual"
            timestamp (date & time):    date & time of this script invocation
            query (string):             Query to run on the Central Storage to get the main data slice
        Returns:
            None.
        """
        log_fname = "../logs/generate_datamart_data.log" + timestamp
        logging.basicConfig(filename=log_fname, filemode='w', level=logging.INFO)
 
        self.spark = SparkSession \
                    .builder \
                    .appName('generate-data-for-datamart') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()
        self.mode = "append"
        self.dm_query = query



    #TODO:
    # Read data by a date period and saving it to a temp parquet file (when in "manual" run mode)
    def read_data_from_central_storage(self, query):
        connector = PostgresConnector(EndPoint.CENTRAL_STORAGE.value)
        df = connector.read_from_db(self.spark, query)
        # df.printSchema()
        # df.show(5, True)
        return df



    def read_data_from_datamart(self, query):
        connector = PostgresConnector(EndPoint.PROTESTS_DATAMART.value)
        df = connector.read_from_db(self.spark, query)
        # df.printSchema()
        # df.show(5, True)
        return df



    def initialize_table_columns(self):
        """Creates a dictionary of (table name, table columns) key-value pairs
        Args:
            None.
        Returns:
            table_cols_dict (dictionary of column names):  
        """
        table_cols_dict = dict()

        table_cols_dict["Event_Fact"] = ["GoldsteinScale", "NumMentions", "NumSources", "NumArticles", \
                                         "AvgTone", "SqlDate", "DateAdded", "GlobalEventId"]

        table_cols_dict["Mention_Dim"] = ["MentionType", "MentionSourceName", "MentionDocLen", \
                                          "MentionDocTone", "GlobalEventId"]

        table_cols_dict["Event_Dim"] = ["event_id", "IsRootEvent", "EventCode", "EventBaseCode", \
                                        "EventRootCode", "QuadClass", "SourceUrl"]

        table_cols_dict["Actor1_Dim"] = ["actor1_id", "Actor1Code", "Actor1Name", "Actor1CountryCode", \
                                       "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code", \
                                       "Actor1Religion2Code", "Actor1Type1Code"]

        table_cols_dict["Actor2_Dim"] = ["actor2_id", "Actor2Code", "Actor2Name", "Actor2CountryCode", \
                                         "Actor2KnownGroupCode", "Actor2EthnicCode", "Actor2Religion1Code", \
                                         "Actor2Religion2Code", "Actor2Type1Code"]
        
        table_cols_dict["Actor1Geo_Dim"] = ["actor1geo_id", "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", \
                                            "Actor1Geo_ADM1Code", "Actor1Geo_Lat", "Actor1Geo_Long"]

        table_cols_dict["Actor2Geo_Dim"] = ["actor2geo_id", "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", \
                                            "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long"]
        
        table_cols_dict["ActionGeo_Dim"] = ["actiongeo_id", "ActionGeo_Type", "ActionGeo_FullName",  \
                                            "ActionGeo_CountryCode", "ActionGeo_Lat", "ActionGeo_Long"]
        
        return table_cols_dict



    def populate_datamart_tables(self, gdelt_events_df):
        """Takes ... and pulls in data for the fact and dimentions tables.
        Args:
            gdelt_events_df (fata frame): contains data from the GDELT events table.
        Returns:
            None.
        """        
        table_cols_dict = self.initialize_table_columns()

        try:
            # Retrieve GlobalEventIds from GDELT Events dataframe
            event_id_rows = gdelt_events_df.select("GlobalEventId").collect()
            # 'event_ids_list' will be a string of ids, ex: "'926862758','926862759', ..."
            event_ids_list = ["'" + row.GlobalEventId + "'" for row in event_id_rows]
            # convert the list to a string of comma-separated ids
            event_ids_list = ",".join(event_ids_list)

            # First, ppopulate the "Event_Fact" and get all composite primary key pieces back
            table_name = "Event_Fact"
            cols = table_cols_dict[table_name]
            df = gdelt_events_df.select(*cols)

            self.save_table_to_datamart(df, table_name)
            print("\n=== Saved to " + table_name + " table:\n" + df._jdf.showString(20, 20, False))
            logging.info("\n=== Saved to " + table_name + " table:\n" + df._jdf.showString(20, 20, False))
            
            column_names = "event_id, actor1_id, actor2_id, actor1geo_id, " \
                        "actor2geo_id, actiongeo_id, \"GlobalEventId\""
            where_clause = " where \"GlobalEventId\" in (" + event_ids_list + ")"
            query = "(select " + column_names + " from " + table_name + where_clause + ") event_dims_ids"
            event_fact_df = self.read_data_from_datamart(query)

            print("======= Event Facts DataFrame: ")
            event_fact_df.show()

            for table in table_cols_dict.keys():
                if table == "Event_Fact":
                    continue

                cols = table_cols_dict[table]

                if table == "Mention_Dim":
                    table_name = "gdelt_mentions_delta"
                    where_clause = " where \"GlobalEventId\" in (" + event_ids_list + ")"
                    query = "(select * from " + table_name + where_clause + ") gmd_view"
                    gdelt_mention_df = self.read_data_from_central_storage(query)
                    df = gdelt_mention_df.join(event_fact_df, "GlobalEventId", 'inner') \
                                        .select(*cols) \
                                        .withColumn("MentionDocLen", F.col("MentionDocLen").cast("int")) \
                                        .withColumn("MentionDocTone", F.col("MentionDocTone").cast("int"))
                else:
                    df = gdelt_events_df.join(event_fact_df, "GlobalEventId", 'inner') \
                                        .select(*cols)
                df.show()
                self.save_table_to_datamart(df, table)
                print("\n=== Saved to " + table_name + " table:\n" + df._jdf.showString(20, 20, False))
                logging.info("\n=== Saved to " + table_name + " table:\n" + df._jdf.showString(20, 20, False))
        except IOError as err:
            sys.stderr.write("IO error when saving to database: {0}".format(err))
            logging.warning("IO error when saving to database: {0}".format(err))



    def save_table_to_datamart(self, df, table):
        """Saves the content of dataframe to the Datamart database.
        Args:
            df (data frame):    data frame with selected data
            table (string):     name of the table to save to in the Datamart database
        Returns:
            None
        """
        try:
            connector = PostgresConnector(EndPoint.PROTESTS_DATAMART.value)
            connector.write_to_db(df, table, self.mode)
        except IOError as err:
            sys.stderr.write("IO error when saving to database: {0}".format(err))
            logging.warning("IO error when saving to database: {0}".format(err))



    def run(self):
        """ Defines execution flow on script invocation.
        Args:    None.
        Returns: None.
        """        
        dw_df = self.read_data_from_central_storage(self.dm_query)
        self.populate_datamart_tables(dw_df)

################## End Of GenerateAndSaveDataToDatamart class ################

def main():
    if len(sys.argv) == 2 and \
       sys.argv[1] in ["manual", "schedule"]:
       
        run_type = sys.argv[1]
        datetime_now = datetime.now()
        date_time = datetime_now.strftime("%Y-%m-%d %H:%M:%S.%f")
        timestamp = datetime_now.strftime("%d%m%Y%H%M%S")

        #TODO: need to implement this
        if run_type == "manual":
            table_name = "gdelt_events"
            start_date = '1900-01-01'
            end_date = '1900-01-01'
            where_clause = " where \"EventRootCode\" like '14%'" \
                           " and \"SqlDate\" >= " + start_date + \
                           " and \"SqlDate\" < " + end_date
        elif run_type == "schedule":
            table_name = "gdelt_events_delta"
            where_clause = " where \"EventRootCode\" like '14%'"
        # elif run_type == "date_range":
        #     pass

        query = "(select * from " + table_name + where_clause + ") ge_view"

        process = GenerateAndSaveDataToDatamart(run_type, timestamp, query)

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
