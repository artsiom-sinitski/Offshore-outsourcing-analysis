"""
Purpose: Retrieve the data from AWS S3 bucket, transform the dataset and store it in PostgreSQL database.  
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/03/2020
"""

from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from schema import GdeltDataSchema
from postgres_connector import PostgresConnector
import logging
import boto3
import time


class PreprocessAndSaveDataToDB():

    def __init__(self):
        logging.basicConfig(level=logging.INFO)

        self.spark = SparkSession \
                    .builder \
                    .appName('store-data-to-db') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()

        # self.s3_bucket_name = "gdelt-v1"
        self.s3_bucket_name = "gdelt-v2"
        self.s3_bucket_url = "s3a://" + self.s3_bucket_name + '/'

        self.delimeter = '\t'

        self.events_table = 'gdelt_events'
        self.mentions_table = 'gdelt_mentions'
        self.gkg_table = 'gdelt_gkg'

        self.mode = 'append'


    def read_csv_from_s3(self, file_path):
        """
        Reads the data from S3 storage and loads it into Spark's data frame object
        """
        schema = None
        schema_type = None

        if file_path.endswith(".export.CSV"):
            schema = GdeltDataSchema().getEventSchema()
            schema_type = "event"
        elif file_path.endswith(".mentions.CSV"):
            schema = GdeltDataSchema().getMentionSchema()
            schema_type = "mention"
        elif file_path.endswith(".gkg.csv"):
            schema = GdeltDataSchema().getGkgSchema()
            schema_type = "gkg"

        df = self.spark.read\
                        .format('csv')\
                        .options(header='false', inferSchema='false', sep=self.delimeter)\
                        .schema(schema)\
                        .load(file_path)
        
        return df, schema_type


    def transform_df(self, df, schema_type):
        """
        Cast data frame column values to match the database table column data types.
        """
        if schema_type == "event":
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

        elif schema_type == "mention":
            df = df.withColumn('GlobalEventId', df.GlobalEventId.cast('STRING'))
            df = df.withColumn('EventTimeDate', F.to_date(df.EventTimeDate, format="yyyyMMdd HH:mm:ss"))
            df = df.withColumn('MentionTimeDate', F.to_date(df.MentionTimeDate, format="yyyyMMdd HH:mm:ss"))
            df = df.withColumn('MentionType', df.MentionType.cast('STRING'))
            df = df.withColumn('MentionSourceName', df.MentionSourceName.cast('STRING'))
            df = df.withColumn('MentionIdentifier', df.MentionIdentifier.cast('STRING'))
            df = df.withColumn('SentenceID', df.SentenceID.cast('STRING'))
            df = df.withColumn('Actor1CharOffset', df.Actor1CharOffset.cast('STRING'))
            df = df.withColumn('Actor2CharOffset', df.Actor2CharOffset.cast('STRING'))
            df = df.withColumn('ActionCharOffset', df.ActionCharOffset.cast('STRING'))
            df = df.withColumn('InRawText', df.InRawText.cast('STRING'))
            df = df.withColumn('Confidence', df.Confidence.cast('STRING'))
            df = df.withColumn('MentionDocLen', df.MentionDocLen.cast('STRING'))
            df = df.withColumn('MentionDocTone', df.MentionDocTone.cast('STRING'))
            df = df.withColumn('MentionDocTranslationInfo', df.MentionDocTranslationInfo.cast('STRING'))
            df = df.withColumn('Extras', df.Extras.cast('STRING'))

        elif schema_type == "gkg":
            df = df.withColumn('GkgRecordId', df.GkgRecordId.cast('STRING'))
            df = df.withColumn('Date', F.to_date(df.Date, format="yyyyMMdd'T'HH:mm:ss"))
            df = df.withColumn('SourceCollectionIdentifier', df.SourceCollectionIdentifier.cast('STRING'))
            df = df.withColumn('SourceCommonName', df.SourceCommonName.cast('STRING'))
            df = df.withColumn('DocumentIdentifier', df.DocumentIdentifier.cast('STRING'))
            df = df.withColumn('V1Counts', df.V1Counts.cast('STRING'))
            df = df.withColumn('V2Counts', df.V2Counts.cast('STRING'))
            df = df.withColumn('V1Themes', df.V1Themes.cast('STRING'))
            df = df.withColumn('V2Themes', df.V2Themes.cast('STRING'))
            df = df.withColumn('V1Locations', df.V1Locations.cast('STRING'))
            df = df.withColumn('V2Locations', df.V2Locations.cast('STRING'))
            df = df.withColumn('V1Persons', df.V1Persons.cast('STRING'))
            df = df.withColumn('V2Persons', df.V2Persons.cast('STRING'))
            df = df.withColumn('V1Organizations', df.V1Organizations.cast('STRING'))
            df = df.withColumn('V2Organizations', df.V2Organizations.cast('STRING'))
            df = df.withColumn('V2Tone', df.V2Tone.cast('STRING'))
            df = df.withColumn('Dates', df.Dates.cast('STRING'))
            df = df.withColumn('GCAM', df.GCAM.cast('STRING'))
            df = df.withColumn('SharingImage', df.SharingImage.cast('STRING'))
            df = df.withColumn('RelatedImageEmbeds', df.RelatedImageEmbeds.cast('STRING'))
            df = df.withColumn('SocialImageEmbeds', df.SocialImageEmbeds.cast('STRING'))
            df = df.withColumn('SocialVideoEmbeds', df.SocialVideoEmbeds.cast('STRING'))
            df = df.withColumn('Quotations', df.Quotations.cast('STRING'))
            df = df.withColumn('AllNames', df.AllNames.cast('STRING'))
            df = df.withColumn('Amounts', df.Amounts.cast('STRING'))
            df = df.withColumn('TranslationInfo', df.TranslationInfo.cast('STRING'))
            df = df.withColumn('ExtrasXml', df.ExtrasXml.cast('STRING'))
        
        return df


    def write_df_to_db(self, data_frame, schema_type):
        db_table = None
        if schema_type == "event":
            db_table = self.events_table
        elif schema_type == "mention":
            db_table = self.mentions_table
        elif schema_type == "gkg":
            db_table = self.gkg_table

        connector = PostgresConnector()
        connector.write_to_db(data_frame, db_table, self.mode)


    def run(self):
        # os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

        f_counter = 0
        schema_type = None

        s3 = boto3.resource(service_name = 's3')
        bucket = s3.Bucket(self.s3_bucket_name)
        
        for file_name in bucket.objects.all():
            file_path = self.s3_bucket_url + file_name.key

            start_time = time.time()
            in_df, schema_type = self.read_csv_from_s3(file_path)
            # in_df.printSchema()
            # in_df.show(1, False)  # False - don't truncate column's content
  
            out_df = self.transform_df(in_df, schema_type)
            # out_df.printSchema()
            # out_df.show(1, True)

            self.write_df_to_db(out_df, schema_type)
            end_time = time.time()
            
            f_counter += 1
            print("\n>>>>> #" + str(f_counter) + " - '" + file_name.key + "'" +\
                  " processed in %s seconds\n" % round(end_time - start_time, 2))

###################### End of class PreprocessTransferDataToDB ########################
#######################################################################################

def main():
    process = PreprocessAndSaveDataToDB()
    process.run()
    print('\n========== Finished moving data from S3 to database! ==========\n')


if __name__ == '__main__':
    main()
    

    