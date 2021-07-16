class SqlQueries:
    truncate_table = """
        TRUNCATE TABLE {redshift_schema}.{destination_table}
        """

    copy_table = """
        COPY {destination_table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{aws_access_key}'
        SECRET_ACCESS_KEY '{aws_secret_key}'
        REGION '{region}'
        FORMAT AS JSON '{json_format}'
        """

    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
                events.start_time,
                events.userid       AS user_id,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid    AS session_id,
                events.location,
                events.useragent    AS user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                        FROM staging_events
                        WHERE page='NextSong') events
                LEFT JOIN staging_songs songs
                        ON events.song = songs.title
                            AND events.artist = songs.artist_name
                            AND events.length = songs.duration
                WHERE songs.song_id IS NOT NULL
                AND songs.artist_id IS NOT NULL
                ORDER BY events.start_time
    """)

    users_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT A.userId    AS user_id,
                        L.firstName AS first_name,
                        L.lastName  AS last_name,
                        L.gender,
                        L.level
        FROM (SELECT userId, MAX(ts) AS recent FROM staging_events GROUP BY userId HAVING userId IS NOT NULL) A
        LEFT JOIN staging_events L
            ON A.userId = L.userId
            AND A.recent = L.ts
        ORDER BY user_id
    """)

    songs_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)

    artists_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id,
                        MIN(artist_name)      AS name, 
                        MIN(artist_location)  AS location, 
                        MIN(artist_latitude)  AS latitude, 
                        MIN(artist_longitude) AS longitude
        FROM staging_songs
        GROUP BY artist_id
        HAVING artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT start_time::TIMESTAMP WITHOUT TIME ZONE,
                        EXTRACT(HOUR FROM start_time)    AS hour,
                        EXTRACT(DAY FROM start_time)     AS day,
                        EXTRACT(WEEK FROM start_time)    AS week,
                        EXTRACT(MONTH FROM start_time)   AS month,
                        EXTRACT(YEAR FROM start_time)    AS year,
                        TO_CHAR(start_time, 'ID')::INT   AS weekday
        FROM (SELECT start_time FROM songplays) t
        ORDER BY start_time
    """)
