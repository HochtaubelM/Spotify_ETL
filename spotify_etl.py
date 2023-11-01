#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 29 16:53:51 2023

@author: marysia
"""

# import spotipy library
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
# import other relevant libraries
import pandas as pd
import os
import json
from datetime import datetime
import datetime
from sqlalchemy.orm import sessionmaker
import sqlite3
import sqlalchemy




# 
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if the dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key Check: Ensure that 'played_at' is a unique identifier
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    # Chceck if any column contains null values
    if df.isnull().values.any():
        raise Exception("Null values found")
    # # Chcek that all timestamps are of yesterday's date
    # yesterday = datetime.datetime().now() - datetime.timedelta(days=4)
    # yesterday = yesterday.replace(hour = 0, minutes= 0, second=0, microsecond=0)
    
    # timestamps = df['timestamp'].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         raise Exception('At least one of the returned songs does not come from within the last 24 hours')
    
    # return True


def run_spotify_etl():
    # setting up the credentials for Spotify API
    os.environ['SPOTIPY_CLIENT_ID'] = 'your Client ID'
    os.environ['SPOTIPY_CLIENT_SECRET'] = 'your Client Secret'
    os.environ['SPOTIPY_REDIRECT_URI'] = 'your redirect URI'
    # set the location for SQLite database
    DATABASE_LOCATION = "sqlite:////Users/marysia/my_played_tracks.sqlite.db"

    scope = 'user-read-recently-played'
    sp = spotipy.Spotify(auth_manager= SpotifyOAuth(scope=scope))
    
    # Timestamp in miliseconds for 'before' and 'after' argument
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp())*1000
    
    data = sp.current_user_recently_played(limit = 50, before= yesterday_unix_timestamp)
    #data = sp.current_user_recently_played(limit = 50)
    
    # Initialize lists to store information   
    song_names = []
    artist_names = []
    played_at_list = []
    timestamp = []
    duration_s = []# in milliseconds
    release_date = []
    
    # Extract data
    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']["artists"][0]['name'])
        played_at_list.append(song['played_at'])
        timestamp.append(song['played_at'][0:10])
        duration_s.append(song['track']['duration_ms'])
        release_date.append(song['track']['album']['release_date'])
        
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamp,
        "duration" : duration_s,
        "release_date" : release_date
        }
    # Create dataframe
    song_df = pd.DataFrame(song_dict, columns = ['song_name', 'artist_name', 'played_at', 'timestamp', 'duration', 'release_date'])
    
    
    # Validate the data
    
    if check_if_valid_data(song_df):
        print('Data valid, proceed to Load stage')
        
    
    # Load the data
    
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        duration VARCHAR(200),
        release_date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    print('Opened database successfully')
    
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')
        
    conn.close()
    print("Close database successfully")