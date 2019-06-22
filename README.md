## Summary
 A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new
 music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 
 Currently, they don't have an easy way to query their data which resides in S3, in a directory of JSON logs on user 
 activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### So, to resolve this issue:
In this project, 
   - `PySpark` is used to create data frames in Spark.
   - Parquet files are created and saved in output directory.
   - an ETL pipeline is built using Python which will transform data from data files to dimension and fact tables in Spark using "star" schema.
  
### Dimension Tables:
   - users: contains users in the music app.
   - songs: contains songs in the database.
   - artists: contains artists in the database.
   - time: timestamp of records in `songplays` broken down into specific units.
   
### Fact Table:
   - songplays: records in the log data associated with song plays.
   
### Config file:
   - dl.cfg: contains AWS credentials.
   
### ETL Pipeline:
   - reads data from S3, processes that data using Spark, and writes them back to S3.
   
### How to run:
   - run command to install requirements.
        > pip install -r requirements.txt
        
   - run ``etl.py`` to execute the pipeline to reads data from S3, processes that data using Spark, and write them back to S3.
