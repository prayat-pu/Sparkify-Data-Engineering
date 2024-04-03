class SqlQueries:
    songplay_table_insert = ("""
        insert into songplays(user_id, start_time,  level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                events.user_id, 
                events.start_time, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        insert into users (user_id, first_name, last_name ,gender, level)
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        insert into songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        insert into artists(artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        insert into time ( start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    staging_events_table_drop = "drop table if exists events"

    staging_songs_table_drop = "drop table if exists songs"

    songplay_table_drop = "drop table if exists songplays cascade"

    user_table_drop = "drop table if exists users"

    song_table_drop = "drop table if exists songs"

    artist_table_drop = "drop table if exists artists"

    time_table_drop = "drop table if exists time"

    staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        item_in_session INTEGER,
        last_name TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration FLOAT8,
        session_id INT,
        song TEXT,
        status INTEGER,
        ts BIGINT,
        user_agent TEXT,
        user_id INT)
        """)



    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs        INT,
                artist_id        TEXT,
                artist_latitude  FLOAT,
                artist_longitude FLOAT,
                artist_location  TEXT,
                artist_name      TEXT,
                song_id          TEXT,
                title            TEXT,
                duration         FLOAT,
                year             INT
        )
        """)

    songplay_table_create = ("""
        create table if not exists songplays
        (
            songplay_id    bigint identity(1,1) primary key,
            user_id        text not null distkey,
            start_time     timestamp not null sortkey,
            level          varchar(10),
            song_id        text,
            artist_id      text,
            session_id     int,
            location       varchar(100),
            user_agent     text
        ) diststyle key
        """)
    
    user_table_create = ("""
        create table if not exists users 
        (
            user_id     text primary key sortkey,
            first_name  varchar(100) not null,
            last_name   varchar(100) not null,
            gender      varchar(1) not null,
            level       varchar(10) not null
        ) diststyle all
        """)

    song_table_create = ("""
        create table if not exists songs (
            song_id       text primary key sortkey,
            title         varchar(100),
            artist_id     varchar(100) distkey,
            year          smallint,
            duration      float4

        ) diststyle key
        """)

    artist_table_create = ("""
        create table artists (
            artist_id     text primary key sortkey,
            name          varchar(100),
            location      varchar(100),
            latitude      float4,
            longitude     float4
        ) diststyle all
        """)

    time_table_create = ("""
        create table time (
            start_time timestamp primary key sortkey,
            hour       smallint,
            day        smallint,
            week       smallint,
            month      smallint,
            year       smallint distkey,
            weekday    smallint
        ) diststyle key
        """)

    staging_songs_copy = ("""
        copy {0} from '{1}'
        region 'us-east-1'
        access_key_id '{3}'
        SECRET_ACCESS_KEY '{4}'
        json '{2}'
        """)

    staging_events_copy = ("""
        copy {} from '{}'
        region 'us-east-1'
        json '{}'
        access_key_id '{}'
        SECRET_ACCESS_KEY '{}'
        """)