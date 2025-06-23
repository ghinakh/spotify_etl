from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import pandas as pd
import gcsfs
from google.cloud import bigquery, storage
import json
import os


BUCKET = "ghin-data-engineering-project-bucket"
SONGS_PATH = f"gs://{BUCKET}/spotify/mysql_export/songs_export.json"
USERS_PATH = f"gs://{BUCKET}/spotify/mysql_export/users_export.json"
STREAMS_BASE_PATH = f"gs://{BUCKET}/topics/streams_play/year=2025/month=06/day=20/hour=*/*.json"
TEMP_DIR = "/tmp/spotify_data"
PROJECT_ID = "lucky-altar-460913-b7"

with DAG (
    dag_id = "main-dag",
    schedule = "0 8 * * *",
    start_date = datetime(2025, 6, 22, 8, 0),
    catchup=False,
    tags=["portofolio"]
) as dag:
    
    # Task 1: read data from GCS bucket
    @task
    def read_gcs_files():
        os.makedirs(TEMP_DIR, exist_ok=True)
        fs = gcsfs.GCSFileSystem(token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

        # Songs
        with fs.open(SONGS_PATH, 'r') as f:
            songs = [json.loads(line) for line in f if line.strip()]
        with open(f"{TEMP_DIR}/songs.json", "w") as f:
            json.dump(songs, f)

        # Users
        with fs.open(USERS_PATH, 'r') as f:
            users = [json.loads(line) for line in f if line.strip()]
        with open(f"{TEMP_DIR}/users.json", "w") as f:
            json.dump(users, f)

        # Streams
        stream_files = fs.glob(STREAMS_BASE_PATH)
        streams = []
        for file in stream_files:
            with fs.open(file, 'r') as f:
                for line in f:
                    if line.strip():
                        streams.append(json.loads(line))
        with open(f"{TEMP_DIR}/streams.json", "w") as f:
            json.dump(streams, f)

        print("Fetched and saved all JSON files to /tmp/spotify_data")
    
    @task
    def transform_data():
        # Load from /tmp
        with open(f"{TEMP_DIR}/songs.json") as f:
            songs = json.load(f)
        with open(f"{TEMP_DIR}/users.json") as f:
            users = json.load(f)
        with open(f"{TEMP_DIR}/streams.json") as f:
            streams = json.load(f)

        # Transform to DataFrames
        df_songs = pd.DataFrame(songs)
        df_users = pd.DataFrame(users)
        df_streams = pd.DataFrame(streams)

        print(f"Songs: {len(df_songs)} rows")
        print(f"Users: {len(df_users)} rows")
        print(f"Streams: {len(df_streams)} rows")

        # change data type
        df_streams['listen_time'] = pd.to_datetime(df_streams['listen_time'])
        df_users['created_at'] = pd.to_datetime(df_users['created_at'])

        # handle missing values if exists
        def check_handling_nan(df, name):
            any_nan = df.isna().any().any()
            if any_nan:
                # handle nan in that df
                for col in df.columns:
                    if df[col].isna().any():
                        print(f"Handle missing values on df {name}, column {col}")
                        if df[col].dtype == 'object':
                            df[col] = df[col].fillna('Unknown')
                        elif pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].fillna(pd.to_datetime('9999-12-31'))
                        else:
                            df[col] = df[col].fillna(0)
            return df
        
        def check_handling_duplicate(df, id_column, name):
            if df.duplicated(subset=id_column).any():
                print(f"Found duplicate on {name}, drop duplicate...")
                df = df.drop_duplicates(subset=id_column, keep='first')
            print(f"No duplicate on {name}")
            return df
        
        df_streams = check_handling_nan(df_streams, "streams")
        df_users = check_handling_nan(df_users, "users")
        df_songs = check_handling_nan(df_songs, "songs")

        df_songs = check_handling_duplicate(df_songs, id_column='track_id', name="songs")
        df_users = check_handling_duplicate(df_users, id_column='user_id', name="users")
        df_streams = check_handling_duplicate(df_streams, id_column=['user_id', 'track_id', 'listen_time'], name="streams")

        # main table for analysis
        streams_full_clean_analysis = df_streams.merge(df_users, on="user_id", how="inner") \
                                                .merge(df_songs, on="track_id", how="inner")

        # temp table for completeness
        streams_full_archieve = df_streams.merge(df_users, on="user_id", how="left") \
                                        .merge(df_songs, on="track_id", how="left")
        
        missing_users_df = streams_full_archieve[streams_full_archieve['user_id'].isna()]
        missing_songs_df = streams_full_archieve[streams_full_archieve['track_id'].isna()]

        missing_users_count = len(missing_users_df)
        missing_songs_count = len(missing_songs_df)

        print(f"[INFO] Missing users: {missing_users_count}")
        print(f"[INFO] Missing songs: {missing_songs_count}")

        streams_full_clean_analysis.to_csv(f"{TEMP_DIR}/streams_full_cleaned.csv", index=False)

        print("Cleaned, Joined, and saved ON files to /tmp/spotify_data")

    @task
    def create_fact_and_dim():
        # Load cleaned data
        df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv", parse_dates=["listen_time"])
        df_users = pd.read_json(f"{TEMP_DIR}/users.json")
        df_songs = pd.read_json(f"{TEMP_DIR}/songs.json")

        # Create fact_streaming
        df["listen_date"] = df["listen_time"].dt.date
        df["listen_hour"] = df["listen_time"].dt.hour

        fact_streaming = df.copy()

        # Add age_group to dim_users
        bins = list(range(10, 101 + 1, 10))
        labels = [f"{i}-{i+9}" for i in bins[:-1]]
        df_users["age_group"] = pd.cut(
            df_users["user_age"],
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True
        )
        df_users["age_group"] = df_users["age_group"].astype(str)

        # is_new_weekly
        reference_date = pd.to_datetime("2024-06-25")
        start_of_week = reference_date - pd.Timedelta(days=6)
        df_users["created_at"] = pd.to_datetime(df_users["created_at"])
        df_users["is_new_weekly"] = df_users["created_at"].dt.date.between(
            start_of_week.date(),
            reference_date.date()
        )

        dim_users = df_users.copy()

        # Clean up dim_songs
        if "id" in df_songs.columns:
            df_songs = df_songs.drop(columns=["id"])

        df_songs["duration_s"] = df_songs["duration_ms"] / 1000
        df_songs["duration_min_sec"] = df_songs["duration_ms"].apply(
            lambda x: f"{int(x // 60000)}:{int((x % 60000) // 1000):02d}"
        )
        df_songs["tempo_level"] = pd.cut(
            df_songs["tempo"],
            bins=[0, 90, 120, float("inf")],
            labels=["slow", "medium", "fast"]
        )
        df_songs["tempo_level"] = df_songs["tempo_level"].astype(str)

        dim_songs = df_songs.copy()

        print("Created fact and dimension tables was done successfully!")

        # load cleaned to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET)

        # Upload streams_full_cleaned.csv
        blob = bucket.blob("spotify/cleaned/streams_full_cleaned.csv")
        blob.upload_from_filename(f"{TEMP_DIR}/streams_full_cleaned.csv")
        print("Uploaded streams_full_cleaned.csv to GCS successfully!")

        # Upload users.json
        blob = bucket.blob("spotify/cleaned/users.json")
        blob.upload_from_filename(f"{TEMP_DIR}/users.json")
        print("Uploaded users.json to GCS successfully!")

        # Upload songs.json
        blob = bucket.blob("spotify/cleaned/songs.json")
        blob.upload_from_filename(f"{TEMP_DIR}/songs.json")
        print("Uploaded songs.json to GCS successfully!")


        # load to BigQuery
        client = bigquery.Client()
        client.load_table_from_dataframe(fact_streaming, f'{PROJECT_ID}.spotify.fact_streams').result()
        client.load_table_from_dataframe(dim_users, f'{PROJECT_ID}.spotify.dim_users').result()
        client.load_table_from_dataframe(dim_songs, f'{PROJECT_ID}.spotify.dim_songs').result()

        print("Uploaded fact and dimension tables to BigQuery successfully!")


    @task
    def genre_kpi():
        
        
        # df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv")
        # df_songs = pd.read_csv(f"{TEMP_DIR}/songs.csv")

        # genre_level = df.groupby("track_genre").agg(
        #     total_streams=("listen_time", "count"),
        #     avg_popularity=("popularity", "mean"),
        #     unique_listeners=("user_id", "nunique"),
        #     avg_duration=("duration_ms", "mean")    
        # ).reset_index()

        # genre_explicit = df_songs.groupby("track_genre").agg(
        #     total_track=("track_id", "count"),
        #     total_explicit_track=("explicit", "sum")
        # ).reset_index()
        # genre_explicit['explicit_ratio'] = genre_explicit['total_explicit_track'] / genre_explicit['total_track']

        # genre_kpi = genre_level.merge(genre_explicit, on="track_genre", how="left")

        # most_popular = df.groupby(["track_genre", "track_id"]).agg(
        #     total_streams=("track_id", "count")
        # ).reset_index().sort_values(["track_genre", "total_streams"], ascending=[True, False]) \
        # .drop_duplicates("track_genre", keep="first")

        # genre_kpi = genre_kpi.merge(most_popular[["track_genre", "track_id"]], on="track_genre", how="left")

        # print("Generate Genre-level KPIs was done successfully")

        genre_kpi_query = f"""
        CREATE OR REPLACE VIEW `{PROJECT_ID}.spotify.view_genre_kpi` AS
        WITH genre_stream AS (
            SELECT
                track_genre,
                COUNT(listen_time) AS total_streams,
                AVG(popularity) AS avg_popularity,
                COUNT(DISTINCT user_id) AS unique_listeners,
                AVG(duration_ms) AS avg_duration
            FROM `{PROJECT_ID}.spotify.fact_streams`
            GROUP BY track_genre
        ),
        genre_explicit AS (
            SELECT
                track_genre,
                COUNT(track_id) AS total_track,
                SUM(explicit) AS total_explicit_track,
                SAFE_DIVIDE(SUM(explicit), COUNT(track_id)) AS explicit_ratio
            FROM `{PROJECT_ID}.spotify.dim_songs`
            GROUP BY track_genre
        ),
        track_count AS (
            SELECT
                track_genre,
                track_id,
                COUNT(*) AS track_streams
            FROM `{PROJECT_ID}.spotify.fact_streams`
            GROUP BY track_genre, track_id
        ),
        most_popular AS (
            SELECT
                track_genre,
                ARRAY_AGG(track_id ORDER BY track_streams DESC LIMIT 1)[OFFSET(0)] AS most_popular_track_id
            FROM track_count
            GROUP BY track_genre
        )
        SELECT
            gs.track_genre,
            gs.total_streams,
            gs.avg_popularity,
            gs.unique_listeners,
            gs.avg_duration,
            ge.total_track,
            ge.total_explicit_track,
            ge.explicit_ratio,
            mp.most_popular_track_id
        FROM genre_stream gs
        LEFT JOIN genre_explicit ge ON gs.track_genre = ge.track_genre
        LEFT JOIN most_popular mp ON gs.track_genre = mp.track_genre;
        
        """
        client = bigquery.Client()
        client.query(genre_kpi_query).result()
        print("Genre-level KPI view was created successfully in BigQuery!")

    @task
    def hourly_kpi():
        # df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv", parse_dates=["listen_time"])

        # df["listen_date"] = df["listen_time"].dt.date
        # df["listen_hour"] = df["listen_time"].dt.hour

        # hourly_kpi = df.groupby(["listen_date", "listen_hour"]).agg(
        #     total_streams=("listen_time", "count"),
        #     unique_listeners=("user_id", "nunique")
        # ).reset_index()

        # top_genre = df.groupby(["listen_date", "listen_hour", "track_genre"]).agg(
        #     total_streams=("track_genre", "count")
        # ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        # .drop_duplicates(["listen_date", "listen_hour"])

        # top_artist = df.groupby(["listen_date", "listen_hour", "artists"]).agg(
        #     total_streams=("artists", "count")
        # ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        # .drop_duplicates(["listen_date", "listen_hour"])

        # bins = list(range(10, 101 + 1, 10))
        # labels = [f"{i}-{i+9}" for i in bins[:-1]]
        # df["age_group"] = pd.cut(df["user_age"], bins=bins, labels=labels, right=False, include_lowest=True)

        # top_age = df.groupby(["listen_date", "listen_hour", "age_group"]).agg(
        #     total_streams=("age_group", "count")
        # ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        # .drop_duplicates(["listen_date", "listen_hour"])

        # # Gabungkan semua ke hourly_kpi
        # hourly_kpi = hourly_kpi \
        #     .merge(top_genre[["listen_date", "listen_hour", "track_genre"]], on=["listen_date", "listen_hour"], how="left") \
        #     .merge(top_artist[["listen_date", "listen_hour", "artists"]], on=["listen_date", "listen_hour"], how="left") \
        #     .merge(top_age[["listen_date", "listen_hour", "age_group"]], on=["listen_date", "listen_hour"], how="left")

        # print("Generate hourly-level KPIs was done successfully")
        hourly_kpi_query = f"""
        CREATE OR REPLACE VIEW `{PROJECT_ID}.spotify.view_hourly_kpi` AS
        WITH base_stream AS (
            SELECT
                DATE(listen_time) AS listen_date,
                EXTRACT(HOUR FROM listen_time) AS listen_hour,
                user_id,
                track_genre,
                artists,
                user_age
            FROM `{PROJECT_ID}.spotify.fact_streams`
        ),
        total_streams_per_hour AS (
            SELECT
                listen_date,
                listen_hour,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT user_id) AS unique_listeners
            FROM base_stream
            GROUP BY listen_date, listen_hour
        ),
        top_genre_per_hour AS (
            SELECT
                listen_date,
                listen_hour,
                track_genre,
                COUNT(*) AS total_streams,
                ROW_NUMBER() OVER (PARTITION BY listen_date, listen_hour ORDER BY COUNT(*) DESC) AS genre_rank
            FROM base_stream
            GROUP BY listen_date, listen_hour, track_genre
        ),
        top_artist_per_hour AS (
            SELECT
                listen_date,
                listen_hour,
                artists,
                COUNT(*) AS total_streams,
                ROW_NUMBER() OVER (PARTITION BY listen_date, listen_hour ORDER BY COUNT(*) DESC) AS artist_rank
            FROM base_stream
            GROUP BY listen_date, listen_hour, artists
        ),
        age_grouping AS (
            SELECT
                *,
                CASE
                    WHEN user_age BETWEEN 10 AND 19 THEN '10-19'
                    WHEN user_age BETWEEN 20 AND 29 THEN '20-29'
                    WHEN user_age BETWEEN 30 AND 39 THEN '30-39'
                    WHEN user_age BETWEEN 40 AND 49 THEN '40-49'
                    WHEN user_age BETWEEN 50 AND 59 THEN '50-59'
                    WHEN user_age BETWEEN 60 AND 69 THEN '60-69'
                    WHEN user_age BETWEEN 70 AND 79 THEN '70-79'
                    WHEN user_age BETWEEN 80 AND 89 THEN '80-89'
                    WHEN user_age BETWEEN 90 AND 99 THEN '90-99'
                    ELSE 'Unknown'
                END AS age_group
            FROM base_stream
        ),
        top_age_per_hour AS (
            SELECT
                listen_date,
                listen_hour,
                age_group,
                COUNT(*) AS total_streams,
                ROW_NUMBER() OVER (PARTITION BY listen_date, listen_hour ORDER BY COUNT(*) DESC) AS age_rank
            FROM age_grouping
            GROUP BY listen_date, listen_hour, age_group
        )

        SELECT
            t.listen_date,
            t.listen_hour,
            t.total_streams,
            t.unique_listeners,
            g.track_genre AS top_genre,
            a.artists AS top_artist,
            ag.age_group AS top_age_group
        FROM total_streams_per_hour t
        LEFT JOIN (SELECT listen_date, listen_hour, track_genre FROM top_genre_per_hour WHERE genre_rank = 1) g
            ON t.listen_date = g.listen_date AND t.listen_hour = g.listen_hour
        LEFT JOIN (SELECT listen_date, listen_hour, artists FROM top_artist_per_hour WHERE artist_rank = 1) a
            ON t.listen_date = a.listen_date AND t.listen_hour = a.listen_hour
        LEFT JOIN (SELECT listen_date, listen_hour, age_group FROM top_age_per_hour WHERE age_rank = 1) ag
            ON t.listen_date = ag.listen_date AND t.listen_hour = ag.listen_hour

        """
        client = bigquery.Client()
        client.query(hourly_kpi_query).result()
        print("Hourly-level KPI view was created successfully in BigQuery!")

    @task
    def track_summary_kpi():
        # df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv")

        # track_summary = df.groupby("track_id").agg(
        #     total_streams=("track_id", "count"),
        #     unique_listeners=("user_id", "nunique")
        # ).reset_index()

        # # Ambil data lagu
        # df_songs = pd.read_json(f"{TEMP_DIR}/songs.json")

        # track_summary = track_summary.merge(
        #     df_songs[["track_id", "track_name", "artists", "track_genre", "popularity"]],
        #     on="track_id",
        #     how="left"
        # )

        # print("Track popularity summary was done successfully")

        track_summary_query = f"""
        CREATE OR REPLACE VIEW `{PROJECT_ID}.spotify.view_track_summary` AS
        WITH track_stream_summary AS (
            SELECT
                track_id,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT user_id) AS unique_listeners
            FROM `{PROJECT_ID}.spotify.fact_streams`
            GROUP BY track_id
        )
        SELECT
            tss.track_id,
            ds.track_name,
            ds.artists,
            ds.track_genre,
            ds.popularity,
            tss.total_streams,
            tss.unique_listeners
        FROM track_stream_summary tss
        LEFT JOIN `{PROJECT_ID}.spotify.dim_songs` ds
            ON tss.track_id = ds.track_id
        """

        client = bigquery.Client()
        client.query(track_summary_query).result()
        print("Track summary view was created successfully in BigQuery!")

    @task
    def cleanup_temp_dir():
        import shutil
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
            print(f"TEMP_DIR at {TEMP_DIR} has been cleaned up.")
        else:
            print(f"TEMP_DIR {TEMP_DIR} not found, nothing to clean.")
    
    # Set task dependencies
    read = read_gcs_files()
    transform = transform_data()
    fact_dim = create_fact_and_dim()
    genre = genre_kpi()
    hourly = hourly_kpi()
    track_summary = track_summary_kpi()
    cleanup = cleanup_temp_dir()

    read >> transform >> fact_dim >> [genre, hourly, track_summary] >> cleanup