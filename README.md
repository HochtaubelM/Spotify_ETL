# Spotify_ETL

This project consists of two Python scripts, Spotify_etl.py and Spotify_dag.py, that together perform an ETL process to extract data from the Spotify API, transform it, and load it into a SQLite database. The ETL process is scheduled to run daily using Apache Airflow.

## Spotify_etl.py

### Introduction

The Spotify_etl.py script is responsible for extracting data from the Spotify API, transforming it, and loading it into a SQLite database. This script uses the Spotipy library to interact with the Spotify API and requires valid Spotify client credentials.

### Prerequisites

Before running the script, make sure to set up your Spotify API credentials and specify the SQLite database location.
To get your Spotify ClientSecret and ClientID go to: https://developer.spotify.com/dashboard and create app.

### Functions

check_if_valid_data(df: pd.DataFrame): 
This function checks the validity of the extracted data. It ensures that the DataFrame is not empty, enforces a primary key constraint on the 'played_at' column, and checks for any null values. 
You can also implement additional checks if needed.

run_spotify_etl(): 
This function performs the ETL process. It initializes the Spotify API client using SpotifyOAuth, retrieves recently played tracks, and extracts relevant information such as song names, artist names, timestamps, duration, and release dates. The data is then loaded into a SQLite database.

## Spotify_dag.py

### Introduction

The Spotify_dag.py script defines an Apache Airflow DAG (Directed Acyclic Graph) that schedules the Spotify ETL process to run daily.

### Prerequisites

Ensure that you have Apache Airflow installed and configured correctly. Apache Airflow documentation: https://airflow.apache.org/docs/

