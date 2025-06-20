import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.cloud import storage

# MySQL config 
hostname = "xnw2v.h.filess.io"
database = "spotify_savedlive"
port = "3307"
username = "spotify_savedlive"
password = "d2186b93179c18c4914375982028b84b303a06cc"

# GCS config
bucket_name = 'ghin-data-engineering-project-bucket'  # GCS bucket kamu (pakai dash, bukan underscore)
gcp_service_account_json = 'service_account_key.json'  # file JSON dari GCP

destination_blob_name_songs = 'spotify/mysql_export/songs_export.json'  # path di GCS
local_filename_songs = 'songs_export.json'  # nama file lokal

destination_blob_name_users = 'spotify/mysql_export/users_export.json'  # path di GCS
local_filename_users = 'users_export.json'  # nama file lokal

try:
    connection = mysql.connector.connect(host=hostname, database=database, user=username, password=password, port=port)
    if connection.is_connected():
        print("Connect to MySQL Server successfully!")

        cursor = connection.cursor()

        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        query_users = "SELECT * FROM users;"
        df_users = pd.read_sql(query_users, connection)
        print(f"Retrieved {len(df_users)} rows from users table.")

        df_users.to_json(local_filename_users, orient='records', lines=True)
        print(f"JSON file saved: {local_filename_users}")

        query_songs = "SELECT * FROM songs;"
        df_songs = pd.read_sql(query_songs, connection)
        print(f"Retrieved {len(df_songs)} rows from songs table.")

        df_songs.to_json(local_filename_songs, orient='records', lines=True)
        print(f"JSON file saved: {local_filename_songs}")

        client = storage.Client.from_service_account_json(gcp_service_account_json)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name_users)
        blob.upload_from_filename(local_filename_users)
        print(f"File uploaded to GCS: gs://{bucket_name}/{destination_blob_name_users}")

        blob_2 = bucket.blob(destination_blob_name_songs)
        blob_2.upload_from_filename(local_filename_songs)
        print(f"File uploaded to GCS: gs://{bucket_name}/{destination_blob_name_songs}")

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")