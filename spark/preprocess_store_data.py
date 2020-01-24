"""
Purpose: Retrieve the data from an S3 bucket, transform the dataset and store it in PostgreSQL.  
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
"""

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
import postgres

from pyspark.sql.types import *


class PreprocessTransferDataToDB:
    def __init__(self, file_path):
        self.spark = SparkSession \
                    .builder \
                    .appName('store-data-to-db') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()

        self.file_path = file_path
    
    # def define_gdelt_schema():
    #     table_schema = StructType( [ StructField('GLOBALEVENTID', StringType(), True),
    #                                  StructField('SQLDATE', StringType(), True),
    #                                  StructField('MonthYear', IntegerType(), False),
    #                                ])
    #     return table_schema

    def read_csv_from_s3(self):
        # df = self.spark.read.parquet(self.file_path)

        # 25 columns for now
        table_schema = StructType( [ StructField('GlobalEventID', StringType(), True),
                                     StructField('SqlDate', StringType(), True),
                                     StructField('MonthYear', IntegerType(), False),
                                     StructField('Year', IntegerType(), False), 
                                     StructField('FractionDate', StringType(), False),

                                     StructField('Actor1Code', StringType(), False),
                                     StructField('Actor1Name', StringType(), False),
                                     StructField('Actor1CountryCode', StringType(), False),
                                     StructField('Actor1KnownGroupCode', StringType(), False),
                                     StructField('Actor1EthnicCode', StringType(), False),
                                     StructField('Actor1Religion1Code', StringType(), False),
                                     StructField('Actor1Religion2Code', StringType(), False),
                                     StructField('Actor1Type1Code', StringType(), False),
                                     StructField('Actor1Type2Code', StringType(), False),
                                     StructField('Actor1Type3Code', StringType(), False),

                                     StructField('Actor2Code', StringType(), False),
                                     StructField('Actor2Name', StringType(), False),
                                     StructField('Actor2CountryCode', StringType(), False),
                                     StructField('Actor2KnownGroupCode', StringType(), False),
                                     StructField('Actor2EthnicCode', StringType(), False),
                                     StructField('Actor2Religion1Code', StringType(), False),
                                     StructField('Actor2Religion2Code', StringType(), False),
                                     StructField('Actor2Type1Code', StringType(), False),
                                     StructField('Actor2Type2Code', StringType(), False),
                                     StructField('Actor2Type3Code', StringType(), False)
                                   ])

        df = self.spark.read.format('csv')\
                            .options(header='false', inferSchema='false', delimeter='\t')\
                            .schema(table_schema)\
                            .load(self.file_path)

        df.printSchema()
        print()
        df.show()

        print('\n========== Loaded data into Spark! ==========\n')
        return df
   

    # def transform_df(self, df):
    #     df = df.withColumn('GLOBALEVENTID', df.GLOBALEVENTID.cast('INT'))
    #     df = df.withColumn('SQLDATE', F.to_timestamp(df.SQLDATE, format='yyyyMMdd'))
    #     df = df.withColumn('Actor1Code', df.Actor1Code.cast('STRING'))
    #     df = df.withColumn('Actor1Name', df.Actor1Name.cast('STRING'))
    #     df = df.withColumn('Actor2Code', df.Actor2Code.cast('STRING'))
    #     df = df.withColumn('Actor2Name', df.Actor2Name.cast('STRING'))
    #     df = df.withColumn('EventCode', df.EventCode.cast('STRING'))
    #     df = df.withColumn('SOURCEURL', df.SOURCEURL.cast('STRING'))
    #     df = df.drop('FractionDate','DATEADDED')
    #     return df

    def import_data_to_db(df):
        print('\n========== Loaded data into PostgreSQL! ==========\n')

    def run(self):
            in_df = self.read_csv_from_s3()
            #in_df.printSchema()
            
            # out_df = self.transform_df(parquet_df)
            # out_df.printSchema()
            
            # self.write_events_to_db(out_df)
            print('\n========== Completed script execution! ==========\n')

###################### End of class PreprocessTransferDataToDB ########################
#######################################################################################


def main():
    # bucket_url = './data/'
    # file_name = '20160504.export.CSV'
    # file_path = bucket_url + file_name

    bucket_url = 's3a://gdelt-1/'
    file_name = '20160504.export.CSV'
    file_path = bucket_url + file_name

    process = PreprocessTransferDataToDB(file_path)
    process.run()
    print('\n========== Moved data from S3 to the database! ==========\n')


if __name__ == '__main__':
    main()
    