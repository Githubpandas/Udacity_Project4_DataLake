# Data Lake ETL for Startup Sparkify


## Table of Contents

1. [Project Introduction](#project-introduction)
2. [Technologies](#technologies)
3. [Input & Output Data](#input-&output-data)
4. [Design of sparkifydb Database](#design-of-sparkifydb-database)
5. [Files Launch & ETL Pipeline](#files-launch-&-etl-pipeline)


## 1. Project Introduction

Creating this ETL-Pipeline enables the retrieved song & log data from AWS S3 to be processed on a EMR Spark cluster and the created fact & dimension tables
to be loaded to the user's S3 bucket. The BI or data science team are able to use the tables to execute analysis.

The listeners' preference e.g. favourite artists, music listening periods during a day, intention to purchase a membership may be analyzed based 
on this database, so that the app could push the recommendation more precisely. Giving the customers or listners what they want the most is definitely 
an important goal for a startup like Sparkify to expand its user community.

With increasing user streaming activities and song numbers the local RDBMS can't not handle this large amount of data. Storing the data on distributed nodes
in a cluster is the most convenient and cost sparing way for BI-Department to access databases.

In this project, the raw data are stored in 2 groups, which are log and song data, and they are stored on AWS S3. These data are loaded into the
so-called stagings tables to be prepared for further data ingesting. The fact + dimension tables get the data from the staging tables.


## 2. Technologies

* Driver: python3
* API: pyspark


## 3. Input & Output Data

### 3.1 Data directories:

#### Input:
* Song data: s3a://udacity-dend/song_data

Song data pattern:  
![Song data pattern](/song_data_pattern.JPG)

Song data schema:  
![Song data schema](/song_data_schema.JPG)

* Log data: s3a://udacity-dend/log_data  
Log data schema:  
![Log data schema](/song_data_schema.JPG)

#### Output:
* S3 bucket: s3a://zhonglin-s3-bucket/project4tables/  

### 3.2 Data Description

#### Input data:
* Format: .json
* Song data: detailed infos about available songs and corresponding artists are included.
* Log data: detailed infos about users and their music listening behaviours are included.

#### Output data:
* Format: parquet
* Fact table: songplays - all logs about the songplays
* Dimension tables: songs, artists, users, time


## 4. Design of sparkifydb Database

The target database has the star schema:
- fact table --> songplays table  
- dimension tables --> users, songs, artists & time tables  


## 5. File Launch & ETL Pipeline

Execute etl.py directly

### 5.1 etl.py

contains the funtions to execute copy raw data from S3, processing with Apache Spark & insert into S3 
fact + dimension tables.

#### 5.1.1 main()

Assign variables
input: song & log data from udacity
output: my own aws s3 bucket

#### 5.1.2 create_spark_session()

create a sparksession instance

#### 5.1.3 process_song_data(spark, input_data, output_data)
read, process & load song & artist data

* read song data from aws s3
* create songs & artists dimension tables from read dataset
* write these 2 tables to target s3

args: spark - sparksession, input_data - input data directory, output_data - output data directory

#### 5.1.4 test_tables()
read, process & load user, playing time & songplay data

* read log data from aws s3
* create time & users dimension tables from read dataset
* create songplays fact table from joined datasets
* write these 3 tables to target s3

args: spark - sparksession, input_data - input data directory, output_data - output data directory

### 5.2 dwh.cfg

stores the access data to AWS user: AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY
