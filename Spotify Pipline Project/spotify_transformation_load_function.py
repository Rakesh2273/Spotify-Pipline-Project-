import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 

def extract_album_info(data):
    return [
        {
            'album_id': row['track']['album']['id'],
            'name': row['track']['album']['name'],
            'release_date': row['track']['album']['release_date'],
            'total_tracks': row['track']['album']['total_tracks'],
            'url': row['track']['album']['external_urls']['spotify']
        }
        for row in data['items']
    ]

def extract_artist_info(data):
    return [
        {
            'artist_id': artist['id'],
            'artist_name': artist['name'],
            'external_url': artist['href']
        }
        for row in data['items']
        for artist in row['track']['artists']
    ]

def extract_song_info(data):
    return [
        {
            'song_id': row['track']['id'],
            'song_name': row['track']['name'],
            'duration_ms': row['track']['duration_ms'],
            'url': row['track']['external_urls']['spotify'],
            'popularity': row['track']['popularity'],
            'song_added': row['added_at'],
            'album_id': row['track']['album']['id'],
            'artist_id': row['track']['album']['artists'][0]['id']
        }
        for row in data['items']
    ]

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "spotify-etl-project-darshil"
    prefix = "raw_data/to_processed/"
    
    spotify_data, spotify_keys = [], []
    for file in s3.list_objects(Bucket=bucket_name, Prefix=prefix)['Contents']:
        file_key = file['Key']
        if file_key.endswith(".json"):
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            json_object = json.loads(response['Body'].read())
            spotify_data.append(json_object)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_df = pd.DataFrame(extract_album_info(data)).drop_duplicates(subset=['album_id'])
        artist_df = pd.DataFrame(extract_artist_info(data)).drop_duplicates(subset=['artist_id'])
        song_df = pd.DataFrame(extract_song_info(data))
        
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        for df, folder in zip([song_df, album_df, artist_df], ["songs_data", "album_data", "artist_data"]):
            key = f"transformed_data/{folder}/{folder}_transformed_{datetime.now()}.csv"
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': bucket_name, 'Key': key}
        s3_resource.meta.client.copy(copy_source, bucket_name, f'raw_data/processed/{key.split("/")[-1]}')    
        s3_resource.Object(bucket_name, key).delete()
