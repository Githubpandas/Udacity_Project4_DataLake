# Data Lake ETL for Startup Sparkify


## Table of Contents

* [Project Introduction](#project-introduction)
* [Technologies](#technologies)
* [Data Source](#data-source)
* [Design of sparkifydb Database](#design-of-sparkifydb-database)
* [Files Launch & ETL Pipeline](#files-launch-&-etl-pipeline)


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

### Note:
Because the Redshift inserts data in columnar way, the conventional methods to avoid conflicts, especially duplicated rows should be changed.
Using 
! insert into target_table
! select distinct col1, col2, col3
! from source_table
should cover the most problems which could be solved with "on conflict do nothing" in local databases.
In order to do UPSERT one can refer the trick by the query "user_table_insert" in sql_queries.py.


## 3. Data Source

* Format: .json
* Song data: detailed infos about available songs and corresponding artists are included.
* Log data: detailed infos about users and their music listening behaviours are included.

### Data directories:
#### Input:
* Song data: s3a://udacity-dend/song_data
* Log data: s3a://udacity-dend/log_data
#### Output:
* S3 bucket: s3a://zhonglin-s3-bucket/project4tables/

There are duplicates in data, which can be prevented with sql insert "on conflict do nothing" commando.


## 4. Design of sparkifydb Database

The database has the star schema:
- fact table --> songplays table
- dimension tables --> users, songs, artists & time tables


## 5. Files Launch & ETL Pipeline

Execution sequence: sql_queries.py --> create_tables.py --> etl.py

### 5.1 file sql_queries.py

contains the order strings of the table creation, insertation and search queries.

#### drop_table_queries:

Drop tables if they already exist.

#### create_table_queries:

Create empty stagings and fact + dimension tables and define their conditions such as primary key, distkey & sortkey

#### insert_table_queries:

Insert data from staging tables into fact & dimension tables

* about insert into songplays table:
Note that the staging_event_table & staging_song_table should be left joined together when creating the fact table songplays.
Because I am not sure if all relevant song infos are stored in S3, so it is still defined that only logs which are assigned with
songid and artistid are kept for the songplays table.
* about insert into users table:
The users table should store the latest level information of the users. Thus the level info of the last user log record should be
considered as the current level status. Concret query referred to query: user_table_insert

### 5.2 create_tables.py

drops and creats the tables for a new start to run the etl pipeline (using drop & create queries from sql_queries.py).

### 5.3 etl.py

contains the funtions to execute copy to staging tables & insert into fact + dimension tables.

#### 5.3.1 main()

connects to Redshift cluster and execute load & insert queries

#### 5.3.2 loading_staging_tables()

Load logs & song data from S3 into staging tables

#### 5.3.3 insert_tables()

Insert (transformed) corresponding columns from staging table into destination tables in Redshift

#### 5.3.4 test_tables()

Show table row number  and the first 5 records of each table

### 5.4 dwh.cfg

stores the access data to S3 storage and Redshift cluster.
