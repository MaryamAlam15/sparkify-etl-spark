import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("spark.executor.memory", "4g")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f'{input_data}/song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.format('json').load(song_data)

    # extract columns to create songs table
    songs_df = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    # write songs table to parquet files partitioned by year and artist
    songs_df.coalesce(1).write.format("json").partitionBy(["year", "artist_id"]) \
        .mode("overwrite") \
        .save(f'{output_data}/songs')

    # extract columns to create artists table
    artists_df = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_df.coalesce(1).write.format('json').mode('overwrite').save(f'{output_data}/artists/')

    songs_logs = df.alias('songs_logs')
    songs = songs_df.alias('songs')

    artist_songs = songs_logs.join(songs, songs.artist_id == songs_logs.artist_id)
    artist_songs.select(
        songs['song_id'],
        songs['title'].alias('song_title'),
        songs['duration'].alias('length'),
        songs['artist_id'].alias('artist_id'),
        artist_songs['artist_name'].alias('artist_name'),
        artist_songs['artist_location'].alias('location')
    ).coalesce(1).write.format('json').mode('overwrite').save(f'{output_data}/artists_songs/')


def process_log_data(spark, input_data, output_data):
    # get file path to log data file
    log_data = f'{input_data}/log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')

    # write users table to parquet files
    user_table.coalesce(1).write.format('json').mode('overwrite').save(f'{output_data}/users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )

    # write time table to parquet files partitioned by year and month
    time_table.coalesce(1).write.format("json").partitionBy(["year", "month"]) \
        .mode("overwrite") \
        .save(f'{output_data}/time/year=2018')

    # read in song data to use for songplays table
    song_df = spark.read.format('json').load(f'{output_data}/songs/year=*/artist_id=*')
    song_df.createOrReplaceTempView('songs')

    artists_songs_df = spark.read.format('json').load(f'{output_data}/artists_songs/')

    df.orderBy('artist').show(20)
    artists_songs_df.orderBy('artist_name').show(20)

    songplays_table = df.join(artists_songs_df,
                              [df.artist == artists_songs_df.artist_name,
                               df.song == artists_songs_df.song_title,
                               artists_songs_df.length == df.length], how='left') \
        .withColumn('start_time', get_datetime(df['ts']).cast('timestamp'))

    songplays_table = songplays_table.select(
        songplays_table['userId'].alias('user_id'),
        songplays_table['level'],
        artists_songs_df['song_id'],
        artists_songs_df['artist_id'],
        songplays_table['sessionId'].alias('session_id'),
        df['location'],
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        songplays_table['userAgent'].alias('user_agent'),
    )

    songplays_table.show(20)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.coalesce(1).write.format("json") \
        .partitionBy(["year", "month"]) \
        .mode("overwrite") \
        .save(f'{output_data}/songsplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend/output"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
