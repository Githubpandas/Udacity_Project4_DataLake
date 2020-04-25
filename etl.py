import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, \
                                weekofyear, dayofweek, date_format
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

#  set up credential infos
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    create a sparksession instance
    '''
    spark = SparkSession \
        .builder \
        .appName('zhonglin-spark-session') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # uncomment following code to show only error logs
    # spark.sparkContext.setLogLevel("ERROR")
    
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    read, process & load song & artist data

    - read song data from aws s3
    - create songs & artists dimension tables from read dataset
    - write these 2 tables to target s3

    args: sparksession, input data directory, output data directory
    '''
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # drop duplicates if there are any
    df = df.dropDuplicates()

    # create temporary view to enable sql query
    df.createOrReplaceTempView('songdata')

    # extract columns to create songs table
    songs_table = spark.sql('''
    select distinct
        song_id, title, artist_id, year, duration
    from songdata
    ''')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.\
        partitionBy('year', 'artist_id').\
        mode('overwrite').\
        parquet(output_data + 'star_tables/dim_tables/songs/')

    # extract columns to create artists table
    artists_table = spark.sql('''
        select distinct
            artist_id, artist_name, artist_location,
            artist_latitude, artist_longitude
        from songdata
    ''')

    # write artists table to parquet files
    artists_table.write.\
        mode('overwrite').\
        parquet(output_data + 'star_tables/dim_tables/artists/')


def process_log_data(spark, input_data, output_data):
    '''
    read, process & load user, playing time & songplay data

    - read log data from aws s3
    - create time & users dimension tables from read dataset
    - create songplays fact table from joined datasets
    - write these 3 tables to target s3

    args: sparksession, input data directory, output data directory
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # drop duplicates if there are any
    df = df.dropDuplicates()

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # sort df in time descending order, in order to keep the latest user info
    df = df.orderBy('ts', ascending=False)

    # extract columns for users table
    users_table = \
        df.select('userId', 'firstName', 'lastName', 'gender', 'level').\
        dropDuplicates().\
        filter((df.userId != '') & col('userId').isNotNull())

    # write users table to parquet files
    users_table.write.\
        mode('overwrite').\
        parquet(output_data + 'star_tables/dim_tables/users/')

    # create timestamp column from original timestamp column
    df = df.withColumn('new_ts', F.to_timestamp(df.ts/1000))

    # create temporary view to enable sql query
    df.createOrReplaceTempView('logdata')

    # extract columns to create time table
    time_table = spark.sql('''
        select distinct
            new_ts as start_time,
            hour(new_ts) as hour,
            dayofmonth(new_ts) as day,
            weekofyear(new_ts) as week,
            month(new_ts) as month,
            year(new_ts) as year,
            dayofweek(new_ts) as weekday
        from logdata
    ''')

    # write time table to parquet files partitioned by year and month
    time_table.write.\
        partitionBy('year', 'month').\
        mode('overwrite').\
        parquet(output_data + 'star_tables/dim_tables/time/')

    # extract columns from joined datasets to create songplays table
    songplays_table = spark.sql('''
        select
            l.new_ts as start_time,
            l.userid as user_id,
            l.level as level,
            s.song_id as song_id,
            s.artist_id as artist_id,
            l.sessionid as session_id,
            l.location as location,
            l.useragent as user_agent,
            year(l.new_ts) as year,
            month(l.new_ts) as month
        from logdata l
        left join songdata s
            on l.artist = s.artist_name
            and l.song = s.title
            and l.length = s.duration
        where s.title is not null
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.\
        partitionBy('year', 'month').\
        mode('overwrite').\
        parquet(output_data + 'star_tables/fact_tables/songplays/')


def main():
    '''
    input: song & log data from udacity
    output: my own aws s3 bucket
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://zhonglin-s3-bucket/project4tables/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
