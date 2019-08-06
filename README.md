Purpose: Sparkify a new startup company and it's analytics team wanted to perform some amalysis on their music streaming app to know the songs users are listening to. This project contains the Sparkity data modelling with S3 buckets to Redshift final tables and the entire ETL process to extract, process and load the data into RedShift final analytical tables. This will help users from Sparkify to query the data based on their business requirements and identify the areas to improve their music streaming app.

Project summary: Create the sparkify data in structured way and store it in AWS RedShift tables, so that Sparkify team can query the data from different tables based on their business needs 


Input data : Resides in JSON log and song files which are stored in S3 buckets
DB         : AWS RedShift cluster

ETL Schema : Star

Tables: songs - contains song information including song_id, title, artist_id, year, duration
        artists - contains artist information including artist_id, name, location, lattitude, longitude
        users - contains users information including user_id, first_name, last_name, gender, level
        time - contains song play time information including date and time values
        songplays - Fact table -consists most of the information combined form song and log data files
        staging_events - staging table which contains the data extracted from LOG_DATA json file in S3 bucket
        staging_songs  - staging table which contains the data extracted from SONG_DATA json file in S3 bucket
       
Files:
1. create_tables.py - contains the code for deleting/creating all tables mentioned above in/from RedShift cluster
2. etl.py           - contains the code to Extract the data from JSON files which are in S3 buckets, process it and load into the staging                       and final analytical tables
3. dwh.cfg          - Fill all your AWS DWH RedShift cluster configuration details here before creating and loading the tables
4. Sql_editor.ipynb - Containd SQL queries to get the count and records from different tables


Execution guidelines:
1. Execute create_tables.py first in terminal for deleting/creating all tables mentioned above in/from RedShift cluster
2. Execute etl.py after succesful completion of the first step for loading the data from JSON files into tables
3. Test your results by executing Sql_editor.ipynb. Before executing this code, connect to DB by providing your credentials like username, password, host name, port and DB name 
        