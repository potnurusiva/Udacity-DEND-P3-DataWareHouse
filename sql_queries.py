import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events  (artist VARCHAR distkey,
                                                                             auth VARCHAR,
                                                                             first_name VARCHAR,
                                                                             gender VARCHAR,
                                                                             item_in_session INT,
                                                                             last_name VARCHAR,
                                                                             length DECIMAL(10,6),
                                                                             level VARCHAR,
                                                                             location VARCHAR,
                                                                             method VARCHAR,
                                                                             page VARCHAR,
                                                                             registartion BIGINT,
                                                                             session_id INT,
                                                                             song VARCHAR sortkey,
                                                                             status VARCHAR,
                                                                             ts TIMESTAMP,
                                                                             user_agent VARCHAR,
                                                                             user_id VARCHAR);""")

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs  (num_songs INT, 
                                                                            artist_id VARCHAR NOT NULL distkey, 
                                                                            artist_lattitude DECIMAL(10,6), 
                                                                            artist_longitude DECIMAL(10,6),
                                                                            artist_location VARCHAR,
                                                                            artist_name VARCHAR sortkey,
                                                                            song_id VARCHAR,
                                                                            title VARCHAR, 
                                                                            duration DECIMAL(16,6),
                                                                            year INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                                                                 start_time TIMESTAMP NOT NULL,
                                                                 user_id VARCHAR NOT NULL, 
                                                                 level VARCHAR, 
                                                                 song_id VARCHAR NOT NULL,
                                                                 artist_id VARCHAR NOT NULL, 
                                                                 session_id INT, 
                                                                 location VARCHAR, 
                                                                 user_agent VARCHAR);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id VARCHAR PRIMARY KEY,
                                                         first_name VARCHAR, 
                                                         last_name VARCHAR,
                                                         gender VARCHAR,
                                                         level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY,
                                                         title VARCHAR, 
                                                         artist_id VARCHAR NOT NULL,
                                                         year INT NOT NULL, 
                                                         duration DECIMAL(10,6));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY,
                                                             name VARCHAR, 
                                                             location VARCHAR, 
                                                             lattitude DECIMAL(10,6), 
                                                             longitude DECIMAL(10,6) );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, 
                                                        hour INT, 
                                                        day INT, 
                                                        week INT, 
                                                        month INT, 
                                                        year INT, 
                                                        weekday INT);""")

# STAGING TABLES

staging_events_copy = ("""COPY {} FROM {}
                        credentials 'aws_iam_role={}' 
                        format as json {} 
                        TIMEFORMAT as 'epochmillisecs'
                        region 'us-west-2' """).format("staging_events",LOG_DATA,DWH_ARN,LOG_JSONPATH)

staging_songs_copy = ("""COPY {} FROM {}
                        credentials 'aws_iam_role={}' 
                        format as json 'auto' 
                        region 'us-west-2' """).format("staging_songs",SONG_DATA,DWH_ARN)
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        SELECT ts AS start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
                        FROM staging_events
                        JOIN staging_songs
                        ON staging_events.artist = staging_songs.artist_name AND
                           staging_events.song = staging_songs.title; 
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, first_name, last_name, gender, level
                        FROM staging_events WHERE user_id IS NOT NULL;
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, 
                               artist_lattitude AS lattitude, artist_longitude AS longitude
                        FROM staging_songs;
                    """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT ts AS start_time, EXTRACT(hour FROM ts) AS hour,
                                                 EXTRACT(day FROM ts) AS day,
                                                 EXTRACT(week FROM ts) AS week,
                                                 EXTRACT(month FROM ts) AS month,
                                                 EXTRACT(year FROM ts) AS year,
                                                 EXTRACT(weekday FROM ts) AS weekday
                        FROM staging_events WHERE page = 'NextSong';
                    """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
