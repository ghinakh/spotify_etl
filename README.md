# **Spotify Data Engineering Pipeline**

## **Project Overview**
This project is an end-to-end data engineering pipeline simulation designed to process Spotify streaming data using Apache Airflow and various modern cloud components.

The project is inspired by the Spotify dataset available on GitHub by [Mr. Sid Raghunath](https://github.com/sidoncloud/udemy-aws-de-labs/tree/main/Lab1-Airflow-redshift-dyanmo-spotifySongs/data), but it has been further developed into a more realistic scenario by adding data ingestion via Kafka, storing user and song data in MySQL, and utilizing Google Cloud Storage (GCS) and Google BigQuery for the data warehouse and analytics layer.

The entire pipeline is fully orchestrated using Astronomer Airflow running locally via Docker.

## **Data Source & Scenario**
### **Source Dataset:**
Original dataset from GitHub:
[Mr. Sid's Spotify Dataset](https://github.com/sidoncloud/udemy-aws-de-labs/tree/main/Lab1-Airflow-redshift-dyanmo-spotifySongs/data)

### **Simulated Data Flow:**
- Songs & Users:
    - Stored in MySQL Database to simulate an OLTP system.
- Streams:
    - Produced using Confluent Cloud Kafka to simulate real-time streaming ingestion.
    - Delivered to Google Cloud Storage (GCS) via Kafka Sink Connector in JSON format.

### **Final Workflow:**
Data stored in GCS (in JSON format) is scheduled and processed using Apache Airflow Astronomer.

## **Tools & Technologies**
- Apache Airflow (via Astronomer)
- Docker
- Google Cloud Storage (GCS)
- Google BigQuery
- MySQL (OLTP Simulation)
- Confluent Cloud Kafka
- Python (Pandas, Google Cloud SDK)
- Jupyter Notebook
- Google Cloud IAM (Service Accounts)

## **Pipeline Stages:**
![architecture-of-spotify-etl-pipeline](solution_architecture.png "Spotify ETL Pipeline Architecture")
1. Preparing Scenario:
    - Songs & Users → MySQL → GCS
    - Streams → Confluent Cloud Kafka → GCS
2. Data Ingestion:
    - Read songs, users, and streams from GCS
3. Data Cleaning & Transformation:
    - Validate and handle data types
    - Handle missing and duplicate values
    - Join datasets
4. Data Loading:
    - Upload cleaned dataframes (streams_full_cleaned, df_users, df_songs) to GCS.
    - Load fact and dimension tables (fact_streams, dim_users, dim_songs) into BigQuery for analytical processing.
5. View Creation:
    - Create analytical views:
        - view_genre_kpi
        - view_hourly_kpi
        - view_track_summary
    - Views are automatically updated based on the latest tables.

## **Key Metrics Generated**
- Genre KPI: Total Streams, Average Popularity, Unique Listeners, Average Duration, Total Tracks, Total Explicit Tracks, Explicit Ratio, and Most Popular Track per Genre.

- Hourly KPI: Total Streams per Hour, Unique Listeners per Hour, Top Genre, Top Artist, and Top Age Group per Hour.

- Track Summary KPI: Total Streams per Track, Unique Listeners per Track, including additional information such as Artist, Genre, and Track Popularity.
    
## **Schedule**
The pipeline is scheduled to run daily at 05:00 UTC (13:00 WIB).

## **Additional Notes**
- This project is executed using local Airflow with Docker via Astronomer CLI.
- Google Cloud environments are configured using separate Service Account JSONs with minimal required roles:
    - Service Account 1 (for Kafka to GCS): Storage Admin, Storage Object Creator, Storage Object Viewer
    - Service Account 2 (for Airflow to BigQuery): Storage Object User, BigQuery Job User, BigQuery Data Viewer, BigQuery Data Editor


