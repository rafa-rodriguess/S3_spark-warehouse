import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates the spark session used in all the code

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Function to process song data
    
    spark  : The Spark session that will be used to execute commands.
    input_data : The input data to be processed.
    output_data : The location where to store the parquet tables.
    
    Creates Song and artists tables in parquet format
    """
    # read song data file
    df = spark.read.json(input_data)
    

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') 
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + config.get('OUTPUT','SONGTABLE'), 
                                partitionBy=['year', 'artist_id'], 
                                mode='Overwrite')
    
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + config.get('OUTPUT','ARTISTTABLE'), mode='Overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Function to process log data
    
    spark  : The Spark session that will be used to execute commands.
    input_data : The input data to be processed.
    output_data : The location where to store the parquet tables.
    
    Creates User, Time and Songplays tables in parquet format
    """
    # read log data file
    df = spark.read.json(input_data)    
 
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    df_user_unique = df.select('ts', 'userId').groupBy('userId').agg(F.max('ts').alias("ts"))
    user_table  = df.join(df_user_unique, on=['userId', 'ts'], how='inner').select('userId', 'firstName', 'lastName', 'gender', 'level')      
   
    # write users table to parquet files
    user_table.write.parquet(output_data + config.get('OUTPUT','USERTABLE'), mode='Overwrite')
    
    # define functions for extracting time components from ts field
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    get_hour = F.udf(lambda x: x.hour, T.IntegerType()) 
    get_day = F.udf(lambda x: x.day, T.IntegerType()) 
    get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = F.udf(lambda x: x.month, T.IntegerType()) 
    get_year = F.udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType()) 
    
    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", get_timestamp(df.ts))
    df = df.withColumn("hour", get_hour(df.start_time))
    df = df.withColumn("day", get_day(df.start_time))
    df = df.withColumn("week", get_week(df.start_time))
    df = df.withColumn("month", get_month(df.start_time))
    df = df.withColumn("year", get_year(df.start_time))
    df = df.withColumn("weekday", get_weekday(df.start_time))


    # extract columns to create time table
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_table = time_table.dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + config.get('OUTPUT','TIMETABLE'), mode='Overwrite', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.load(output_data + config.get('OUTPUT','SONGTABLE'))
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table  = df.join(song_df, df.song == song_df.title, how='inner').select(df.start_time,df.userId,df.level,song_df.song_id,song_df.artist_id,df.sessionId,df.location,df.userAgent, df.year, df.month)     
    
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + config.get('OUTPUT','SONGPLAY'), mode='Overwrite', partitionBy=['year', 'month'])

def main():
    """   
    - Gets the config info from dwh.cfg
    
    - Creates Spark Session
    
    - Loads Data from S3 Bucket and saves it to another S3 Bucket
    
    """
    songPath = config.get('SOURCE','SONGPATH')
    logPath  = config.get('SOURCE','LOGPATH')
    output_data = config.get('OUTPUT','OUTPUT_PATH')
       
    
    spark = create_spark_session()
    
   
    process_song_data(spark, songPath, output_data)    
    process_log_data(spark, logPath, output_data)


if __name__ == "__main__":
    main()
