import mysql.connector
from mysql.connector import Error
import pandas as pd

# csv file path
csv_songs_path = "data/songs.csv"
csv_users_path = "data/users.csv"

# table name
table_songs = "songs"
table_users = "users"

songs = pd.read_csv(csv_songs_path)
users =pd.read_csv(csv_users_path)


hostname = "xnw2v.h.filess.io"
database = "spotify_savedlive"
port = "3307"
username = "spotify_savedlive"
password = "d2186b93179c18c4914375982028b84b303a06cc"

try:
    connection = mysql.connector.connect(host=hostname, database=database, user=username, password=password, port=port)
    if connection.is_connected():
        print("Connect to MySQL Server successfully!")

        cursor = connection.cursor()

        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        cursor.execute(f"DROP TABLE IF EXISTS {table_songs};")
        print(f"Table {table_songs} removed if exist")

        cursor.execute(f"DROP TABLE IF EXISTS {table_users};")
        print(f"Table {table_users} removed if exist")

        create_table_songs = f"""
        CREATE TABLE {table_songs} (
            id INT,
            track_id VARCHAR(255),
            artists TEXT,
            album_name TEXT,
            track_name TEXT,
            popularity INT,
            duration_ms INT,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            `key` INT,
            loudness FLOAT,
            mode INT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INT,
            track_genre VARCHAR(255)
        );
        """

        create_table_users = f"""
        CREATE TABLE {table_users} (
            user_id INT,
            user_name VARCHAR(255),
            user_age INT,
            user_country VARCHAR(255),
            created_at VARCHAR(255)
        );
        """
        cursor.execute(create_table_songs)
        print(f"Table {table_songs} created successfully!")

        cursor.execute(create_table_users)
        print(f"Table {table_users} created successfully!")

        # insert data songs in batches of 1000 records
        batch_size = 1000
        num_batches_songs = len(songs) // batch_size + 1

        for i in range(num_batches_songs):
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            batch_songs = songs.iloc[start_idx:end_idx]
            batch_songs = batch_songs.where(pd.notnull(batch_songs), None)
            batch_records = [tuple(row) for row in batch_songs.to_numpy()]

            insert_songs_query = f"""INSERT INTO {table_songs} (id, track_id, artists, album_name, track_name,
            popularity, duration_ms, explicit, danceability, energy, `key`, loudness, mode, speechiness, acousticness,
            instrumentalness, liveness, valence, tempo, time_signature, track_genre)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

            cursor.executemany(insert_songs_query, batch_records)
            connection.commit()
            print(f"Batch {i+1}/{num_batches_songs} inserted successfully!")

        print(f"All {len(songs)} songs data inserted successfully!")

        # insert data users in batches of 1000 records
        batch_size = 1000
        num_batches_users = len(users) // batch_size + 1

        for i in range(num_batches_users):
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            batch_users = users.iloc[start_idx:end_idx]
            batch_users = batch_users.where(pd.notnull(batch_users), None)
            batch_users_records = [tuple(row) for row in batch_users.to_numpy()]

            insert_users_query = f"""INSERT INTO {table_users} (user_id, user_name, user_age, user_country, created_at)
            VALUES (%s, %s, %s, %s, %s);"""

            cursor.executemany(insert_users_query, batch_users_records)
            connection.commit()
            print(f"Batch {i+1}/{num_batches_users} inserted successfully!")

        print(f"All {len(users)} users data inserted successfully!")


except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
