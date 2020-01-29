"""
Purpose: Retrieve the data from AWS S3 bucket, transform the dataset and store it in PostgreSQL database.  
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
"""
from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField,\
                              StringType, IntegerType,\
                              DateType, BooleanType,\
                              FloatType
from postgres_connector import PostgresConnector
import logging


class PreprocessAndTransferDataToDB(object):

    def __init__(self, file_path):
        logging.basicConfig(level=logging.INFO)

        self.spark = SparkSession \
                    .builder \
                    .appName('store-data-to-db') \
                    .config('spark.executor.memory', '2gb') \
                    .getOrCreate()

        self.file_path = file_path
        self.delimeter = '\t'


    def read_csv_from_s3(self):

        schema = StructType([
                            StructField('GlobalEventId', StringType(), False),
                            StructField('SqlDate', StringType(), False),
                            StructField('MonthYear', StringType(), True),
                            StructField('Year', StringType(), True), 
                            StructField('FractionDate', StringType(), True),

                            StructField('Actor1Code', StringType(), True),
                            StructField('Actor1Name', StringType(), True),
                            StructField('Actor1CountryCode', StringType(), True),
                            StructField('Actor1KnownGroupCode', StringType(), True),
                            StructField('Actor1EthnicCode', StringType(), True),
                            StructField('Actor1Religion1Code', StringType(), True),
                            StructField('Actor1Religion2Code', StringType(), True),
                            StructField('Actor1Type1Code', StringType(), True),
                            StructField('Actor1Type2Code', StringType(), True),
                            StructField('Actor1Type3Code', StringType(), True),

                            StructField('Actor2Code', StringType(), True),
                            StructField('Actor2Name', StringType(), True),
                            StructField('Actor2CountryCode', StringType(), True),
                            StructField('Actor2KnownGroupCode', StringType(), True),
                            StructField('Actor2EthnicCode', StringType(), True),
                            StructField('Actor2Religion1Code', StringType(), True),
                            StructField('Actor2Religion2Code', StringType(), True),
                            StructField('Actor2Type1Code', StringType(), True),
                            StructField('Actor2Type2Code', StringType(), True),
                            StructField('Actor2Type3Code', StringType(), True),
                            
                            StructField('IsRootEvent', StringType(), True),
                            StructField('EventCode', StringType(), True),
                            StructField('EventBaseCode', StringType(), True),
                            StructField('EventRootCode', StringType(), True),
                            StructField('QuadClass', StringType(), True),
                            StructField('GoldsteinScale', StringType(), True),
                            StructField('NumMentions', StringType(), True),
                            StructField('NumSources', StringType(), True),
                            StructField('NumArticles', StringType(), True),
                            StructField('AvgTone', StringType(), True),
                            
                            StructField('Actor1Geo_Type', StringType(), True),
                            StructField('Actor1Geo_FullName', StringType(), True),
                            StructField('Actor1Geo_CountryCode', StringType(), True),
                            StructField('Actor1Geo_ADM1Code', StringType(), True),
                            StructField('Actor1Geo_Lat', StringType(), True),
                            StructField('Actor1Geo_Long', StringType(), True),
                            StructField('Actor1Geo_FeatureID', StringType(), True),
                            
                            StructField('Actor2Geo_Type', StringType(), True),
                            StructField('Actor2Geo_FullName', StringType(), True),
                            StructField('Actor2Geo_CountryCode', StringType(), True),
                            StructField('Actor2Geo_ADM1Code', StringType(), True),
                            StructField('Actor2Geo_Lat', StringType(), True),
                            StructField('Actor2Geo_Long', StringType(), True),
                            StructField('Actor2Geo_FeatureID', StringType(), True),
                            
                            StructField('ActionGeo_Type', StringType(), True),
                            StructField('ActionGeo_FullName', StringType(), True),
                            StructField('ActionGeo_CountryCode', StringType(), True),
                            StructField('ActionGeo_ADM1Code', StringType(), True),
                            StructField('ActionGeo_Lat', StringType(), True),
                            StructField('ActionGeo_Long', StringType(), True),
                            StructField('ActionGeo_FeatureID', StringType(), True),
                            
                            StructField('DateAdded', StringType(), True),
                            StructField('SourceUrl', StringType(), True)
                            ])
        
        df = self.spark.read\
                       .format('csv')\
                       .options(header='false', inferSchema='false', sep=self.delimeter)\
                       .schema(schema)\
                       .load(self.file_path)
        print('\n========== Loaded data into Spark object! ==========\n')
        return df
   

    def transform_df(self, df):
        """
        Cast the df column values to match the database table column data types.
        """
        df = df.withColumn('GlobalEventId', df.GlobalEventId.cast('STRING'))
        df = df.withColumn('SqlDate', F.to_date(df.SqlDate, format='yyyyMMdd'))
        df = df.withColumn('MonthYear', df.MonthYear.cast('INT'))
        df = df.withColumn('Year', df.Year.cast('INT'))
        df = df.withColumn('FractionDate', df.FractionDate.cast('STRING'))

        df = df.withColumn('Actor1Code', df.Actor1Code.cast('STRING'))
        df = df.withColumn('Actor1Name', df.Actor1Name.cast('STRING'))
        df = df.withColumn('Actor1CountryCode', df.Actor1CountryCode.cast('STRING'))
        df = df.withColumn('Actor1KnownGroupCode', df.Actor1KnownGroupCode.cast('STRING'))
        df = df.withColumn('Actor1EthnicCode', df.Actor1EthnicCode.cast('STRING'))
        df = df.withColumn('Actor1Religion1Code', df.Actor1Religion1Code.cast('STRING'))
        df = df.withColumn('Actor1Religion2Code', df.Actor1Religion2Code.cast('STRING'))
        df = df.withColumn('Actor1Type1Code', df.Actor1Type1Code.cast('STRING'))
        df = df.withColumn('Actor1Type2Code', df.Actor1Type2Code.cast('STRING'))
        df = df.withColumn('Actor1Type3Code', df.Actor1Type3Code.cast('STRING'))

        df = df.withColumn('Actor2Code', df.Actor2Code.cast('STRING'))
        df = df.withColumn('Actor2Name', df.Actor2Name.cast('STRING'))
        df = df.withColumn('Actor2CountryCode', df.Actor2CountryCode.cast('STRING'))
        df = df.withColumn('Actor2KnownGroupCode', df.Actor2KnownGroupCode.cast('STRING'))
        df = df.withColumn('Actor2EthnicCode', df.Actor2EthnicCode.cast('STRING'))
        df = df.withColumn('Actor2Religion1Code', df.Actor2Religion1Code.cast('STRING'))
        df = df.withColumn('Actor2Religion2Code', df.Actor2Religion2Code.cast('STRING'))
        df = df.withColumn('Actor2Type1Code', df.Actor2Type1Code.cast('STRING'))
        df = df.withColumn('Actor2Type2Code', df.Actor2Type2Code.cast('STRING'))
        df = df.withColumn('Actor2Type3Code', df.Actor2Type3Code.cast('STRING'))

        df = df.withColumn('IsRootEvent', df.IsRootEvent.cast('BOOLEAN'))
        df = df.withColumn('EventCode', df.EventCode.cast('STRING'))
        df = df.withColumn('EventBaseCode', df.EventBaseCode.cast('STRING'))
        df = df.withColumn('EventRootCode', df.EventRootCode.cast('STRING'))
        df = df.withColumn('QuadClass', df.QuadClass.cast('INT'))
        df = df.withColumn('GoldsteinScale', df.GoldsteinScale.cast('FLOAT'))
        df = df.withColumn('NumMentions', df.NumMentions.cast('INT'))
        df = df.withColumn('NumSources', df.NumSources.cast('INT'))
        df = df.withColumn('NumArticles', df.NumArticles.cast('INT'))
        df = df.withColumn('AvgTone', df.AvgTone.cast('FLOAT'))

        df = df.withColumn('Actor1Geo_Type', df.Actor1Geo_Type.cast('INT'))
        df = df.withColumn('Actor1Geo_FullName', df.Actor1Geo_FullName.cast('STRING'))
        df = df.withColumn('Actor1Geo_CountryCode', df.Actor1Geo_CountryCode.cast('STRING'))
        df = df.withColumn('Actor1Geo_ADM1Code', df.Actor1Geo_ADM1Code.cast('STRING'))
        df = df.withColumn('Actor1Geo_Lat', df.Actor1Geo_Lat.cast('STRING'))
        df = df.withColumn('Actor1Geo_Long', df.Actor1Geo_Long.cast('STRING'))
        df = df.withColumn('Actor1Geo_FeatureID', df.Actor1Geo_FeatureID.cast('STRING'))

        df = df.withColumn('Actor2Geo_Type', df.Actor2Geo_Type.cast('INT'))
        df = df.withColumn('Actor2Geo_FullName', df.Actor2Geo_FullName.cast('STRING'))
        df = df.withColumn('Actor2Geo_CountryCode', df.Actor2Geo_CountryCode.cast('STRING'))
        df = df.withColumn('Actor2Geo_ADM1Code', df.Actor2Geo_ADM1Code.cast('STRING'))
        df = df.withColumn('Actor2Geo_Lat', df.Actor2Geo_Lat.cast('STRING'))
        df = df.withColumn('Actor2Geo_Long', df.Actor2Geo_Long.cast('STRING'))
        df = df.withColumn('Actor2Geo_FeatureID', df.Actor2Geo_FeatureID.cast('STRING'))

        df = df.withColumn('ActionGeo_Type', df.ActionGeo_Type.cast('INT'))
        df = df.withColumn('ActionGeo_FullName', df.ActionGeo_FullName.cast('STRING'))
        df = df.withColumn('ActionGeo_CountryCode', df.ActionGeo_CountryCode.cast('STRING'))
        df = df.withColumn('ActionGeo_ADM1Code', df.ActionGeo_ADM1Code.cast('STRING'))
        df = df.withColumn('ActionGeo_Lat', df.ActionGeo_Lat.cast('STRING'))
        df = df.withColumn('ActionGeo_Long', df.ActionGeo_Long.cast('STRING'))
        df = df.withColumn('ActionGeo_FeatureID', df.ActionGeo_FeatureID.cast('STRING'))

        df = df.withColumn('DateAdded', F.to_date(df.DateAdded, format='yyyyMMdd'))
        df = df.withColumn('SourceUrl', df.SourceUrl.cast('STRING'))

        return df


    def write_events_to_db(self, data_frame):
        db_table = 'gdelt_v1_events'
        mode = 'append'

        connector = PostgresConnector()
        connector.write_to_db(data_frame, db_table, mode)
        print('\n========== Loaded data into PostgreSQL ==========\n')


    def run(self):
        in_df = self.read_csv_from_s3()
        in_df.printSchema()
        in_df.show(3, False)
        
        out_df = self.transform_df(in_df)
        out_df.printSchema()
        out_df.show(3, False)
        
        self.write_events_to_db(out_df)

###################### End of class PreprocessTransferDataToDB ########################
#######################################################################################


def main():
    # s3bucket_url = './data/'
    # file_name = '20160504.export-5.CSV'
    # file_path = s3bucket_url + file_name

    s3bucket_url = 's3a://gdelt-1/'
    file_name = '20160504.export.CSV'
    file_path = s3bucket_url + file_name

    process = PreprocessAndTransferDataToDB(file_path)
    process.run()
    print('\n========== Moved data from S3 to the database! ==========\n')


if __name__ == '__main__':
    main()
    

    