import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import  pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f'{input_data}/song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)
    df.show(5)

    # # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.show(5)
    #
    # # write songs table to parquet files partitioned by year and artist
    songs_table.write.format("parquet").partitionBy(["year", "artist_id"]).mode("overwrite").save(
        f"{output_data}/songs.parquet")
    spark.read.format("parquet").load(f"{output_data}/songs.parquet").show()

    # # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table.show(5)
    #
    # # write artists table to parquet files
    artists_table.write.format('parquet').mode('overwrite').save(f'{output_data}/artists.parquet')


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data = f'{input_data}/log_data/2018/11/*.json'
#
#     # read log data file
#     df = spark.read.json(log_data)
#
#     # filter by actions for song plays
#     df = df.filter("page='NextSong'")
#     # df.show(5)
# #
#     # extract columns for users table
#     user_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
#
#     # write users table to parquet files
#     user_table.write.format('parquet').mode('overwrite').save('output_data/users.parquet')
# #
# #     # create timestamp column from original timestamp column
#     get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
#     df = df.withColumn("timestamp", get_timestamp(df.ts))
#
# #
# #     # create datetime column from original timestamp column
#     get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
#     df = df.withColumn("datetime", get_datetime(df.ts))
# #
# #     # extract columns to create time table
#     time_table = df.select(
#         'timestamp',
#         hour('datetime').alias('hour'),
#         dayofmonth('datetime').alias('day'),
#         weekofyear('datetime').alias('week'),
#         month('datetime').alias('month'),
#         year('datetime').alias('year'),
#         date_format('datetime', 'F').alias('weekday')
#     )
#     # time_table.show(5)
# #
# #     # write time table to parquet files partitioned by year and month
#     time_table.write.format("parquet").partitionBy(["year", "month"]).mode("overwrite").save(
#         "output_data/time.parquet")
#
#     # read in song data to use for songplays table
#     song_df = spark.read.format('parquet').load('output_data/songs.parquet')
#     song_df.createOrReplaceTempView('songs')
#     spark.sql("""
#         select * from songs
#         limit 5
#     """).show()
# #
# #     # extract columns from joined song and log datasets to create songplays table
#     df.createOrReplaceTempView('logs_data_set')
#     spark.sql("""
#         select * from logs_data_set
#         limit 5
#     """).show()
#
#     spark.sql("""
#     with artist_songs as (
#     select
#         s.song_id,
#         s.title as song_title,
#         s.duration as length,
#         s.artist_id,
#         lds.artist_name,
#         s.artist_location as location
#     from logs_data_set lds
#     inner join songs s
#     on lds.artist_id=s.artist_id
# )
# select
#      timestamp 'epoch' + FLOAT8(e.ts)/1000 * interval '1 second' as start_time,
#      e.userId as user_id,
#      e.level,
#      a.song_id,
#      a.artist_id,
#      e.sessionId as session_id,
#      a.location,
#      e.userAgent as user_agent
# from staging_events e
# left join artist_songs a
# on e.artist=a.artist_name
# where e.song=a.song_title
# and e.length=a.length;
#     """
#     ).show()
# #     songplays_table =
# #
# #     # write songplays table to parquet files partitioned by year and month
# #     songplays_table


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "output/"

    process_song_data(spark, input_data, output_data)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
