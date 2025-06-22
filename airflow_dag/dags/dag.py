from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import pandas as pd
import gcsfs
import json
import os


BUCKET = "ghin-data-engineering-project-bucket"
SONGS_PATH = f"gs://{BUCKET}/spotify/mysql_export/songs_export.json"
USERS_PATH = f"gs://{BUCKET}/spotify/mysql_export/users_export.json"
STREAMS_BASE_PATH = f"gs://{BUCKET}/topics/streams_play/year=2025/month=06/day=20/hour=*/*.json"
TEMP_DIR = "/tmp/spotify_data"

with DAG (
    dag_id = "main-dag",
    schedule = "@once",
    start_date = datetime(2025, 6, 22, 3, 0),
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
    def genre_kpi():
        df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv")

        genre_level = df.groupby("track_genre").agg(
            total_streams=("listen_time", "count"),
            avg_popularity=("popularity", "mean"),
            unique_listeners=("user_id", "nunique"),
            avg_duration=("duration_ms", "mean")    
        ).reset_index()

        genre_explicit = df.groupby("track_genre").agg(
            total_track=("track_id", "count"),
            total_explicit_track=("explicit", "sum")
        ).reset_index()
        genre_explicit['explicit_ratio'] = genre_explicit['total_explicit_track'] / genre_explicit['total_track']

        genre_kpi = genre_level.merge(genre_explicit, on="track_genre", how="left")

        most_popular = df.groupby(["track_genre", "track_id"]).agg(
            total_streams=("track_id", "count")
        ).reset_index().sort_values(["track_genre", "total_streams"], ascending=[True, False]) \
        .drop_duplicates("track_genre")

        genre_kpi = genre_kpi.merge(most_popular[["track_genre", "track_id"]], on="track_genre", how="left")

        print("Generate Genre-level KPIs was done successfully")

    @task
    def hourly_kpi():
        df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv", parse_dates=["listen_time"])

        df["listen_date"] = df["listen_time"].dt.date
        df["listen_hour"] = df["listen_time"].dt.hour

        hourly_kpi = df.groupby(["listen_date", "listen_hour"]).agg(
            total_streams=("listen_time", "count"),
            unique_listeners=("user_id", "nunique")
        ).reset_index()

        top_genre = df.groupby(["listen_date", "listen_hour", "track_genre"]).agg(
            total_streams=("track_genre", "count")
        ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        .drop_duplicates(["listen_date", "listen_hour"])

        top_artist = df.groupby(["listen_date", "listen_hour", "artists"]).agg(
            total_streams=("artists", "count")
        ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        .drop_duplicates(["listen_date", "listen_hour"])

        bins = list(range(10, 101 + 1, 10))
        labels = [f"{i}-{i+9}" for i in bins[:-1]]
        df["age_group"] = pd.cut(df["user_age"], bins=bins, labels=labels, right=False, include_lowest=True)

        top_age = df.groupby(["listen_date", "listen_hour", "age_group"]).agg(
            total_streams=("age_group", "count")
        ).reset_index().sort_values(["listen_date", "listen_hour", "total_streams"], ascending=[True, True, False]) \
        .drop_duplicates(["listen_date", "listen_hour"])

        # Gabungkan semua ke hourly_kpi
        hourly_kpi = hourly_kpi \
            .merge(top_genre[["listen_date", "listen_hour", "track_genre"]], on=["listen_date", "listen_hour"], how="left") \
            .merge(top_artist[["listen_date", "listen_hour", "artists"]], on=["listen_date", "listen_hour"], how="left") \
            .merge(top_age[["listen_date", "listen_hour", "age_group"]], on=["listen_date", "listen_hour"], how="left")

        print("Generate hourly-level KPIs was done successfully")

    @task
    def track_summary_kpi():
        df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv")

        track_summary = df.groupby("track_id").agg(
            total_streams=("track_id", "count"),
            unique_listeners=("user_id", "nunique")
        ).reset_index()

        # Ambil data lagu
        df_songs = pd.read_json(f"{TEMP_DIR}/songs.json")

        track_summary = track_summary.merge(
            df_songs[["track_id", "track_name", "artists", "track_genre", "popularity"]],
            on="track_id",
            how="left"
        )

        print("Track popularity summary was done successfully")

    @task
    def create_fact_and_dim():
        # Load cleaned data
        df = pd.read_csv(f"{TEMP_DIR}/streams_full_cleaned.csv", parse_dates=["listen_time"])
        df_users = pd.read_json(f"{TEMP_DIR}/users.json")
        df_songs = pd.read_json(f"{TEMP_DIR}/songs.json")

        # Create fact_streaming
        df["listen_date"] = df["listen_time"].dt.date
        df["listen_hour"] = df["listen_time"].dt.hour

        fact_streaming = df[[
            "user_id", "track_id", "listen_time", "listen_date",
            "listen_hour", "artists", "track_name", "track_genre"
        ]]

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

        dim_songs = df_songs.copy()

        print("Created fact and dimension tables:")

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
    genre = genre_kpi()
    hourly = hourly_kpi()
    track_summary = track_summary_kpi()
    cleanup = cleanup_temp_dir()

    read >> transform >> [genre, hourly, track_summary, create_fact_and_dim()] >> cleanup