import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import datetime
import sqlite3
import requests
import json


from sqlalchemy.sql.expression import except_
refresh_token = "AQA4wV_m1jhTKf1hfp_B8RSfjWmrvXiOUmBr-LyYkiG8hi4EYy9bd4uo7avd-EVtN7l46flshmzYRWaA24LBFAxFmMRi2DQSC5kxhy-IA6ItFYUUkqAOzybN4u1gnVQyM3Q"
base_64 = "ODYzYmU3Y2IyNGJmNDNmMTlhNThmODIxOWNjMjQxYTY6NGI1ZTVjM2ViZTZkNGY0ZWJhY2U4YWM5ZDZkNGZlMzQ="
access_token = "AQAemMS4yQ0IiDySAAb6knOny8SkQhPdywtdMgnyhPmDu25A6VP-H3k04554jxPGyjlVq893FkMpTWBAJ5yQiQWtIpmm75zExKba7bgXc8pDIM8lPASqBmy0RG04v8MHrtk"


def refresh():
    query = "https://accounts.spotify.com/api/token"

    response = requests.post(query,
                             data={"grant_type": "refresh_token",
                                    "refresh_token": refresh_token},
                             headers={"Authorization": "Basic " + base_64})

    response_json = response.json()
    print(response_json)

    return response_json["access_token"]


def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded, Finishing execution")
        return False

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception["Primary Key Check is violated"]

    if df.isnull().values.any():
        raise Exception("Null valued found")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=0)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            print(datetime.datetime.strptime(timestamp, "%Y-%m-%d"))
            print(yesterday)
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True


def  run_spotify_etl():
    refresh_token = refresh()

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {}".format(refresh_token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    print(data)

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dic = {
        "song_names" : song_names,
        "artidy_names" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dic, columns= ["song_names","artidy_names","played_at","timestamp"])

    #VALIDADE
    if check_if_valid_data(song_df):
        print("Data valid, proceed to load stage")
    print(song_df)

    #LOAD
    engine = sqlalchemy.create_engine("sqlite:///my_played_tracks.sqlite")
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXIST my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exist in the database")

    conn.close()
    print("close database successfully")

run_spotify_etl()