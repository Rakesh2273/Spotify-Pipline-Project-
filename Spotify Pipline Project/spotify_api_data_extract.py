import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_uri)
    
    s3_client = boto3.client('s3')
    filename = f"spotify_raw_{datetime.now()}.json"
    
    s3_client.put_object(
        Bucket="spotify-etl-project-darshil",
        Key=f"raw_data/to_processed/{filename}",
        Body=json.dumps(spotify_data)
    )
