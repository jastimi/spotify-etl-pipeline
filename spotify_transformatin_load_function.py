import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list=[]
    for item in data['items']:
        album_id = item['track']['album']['id']
        album_name = item['track']['album']['name']
        album_release_date = item['track']['album']['release_date']
        album_total_tracks = item['track']['album']['total_tracks']
        album_url = item['track']['album']['external_urls']['spotify']
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                        'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list=[]
    for item in data['items']:
        for artist in item['track']['artists']:
            artist_id = artist['id']
            artist_name = artist['name']
            external_uri = artist['href']
            artist_element = {'artist_id': artist_id, 'artist_name': artist_name, 'external_url': external_uri}
            artist_list.append(artist_element)
    return artist_list

def songs(data):
    song_list=[]
    for item in data['items']:
        song_id = item['track']['id']
        song_name = item['track']['name']
        song_duration = item['track']['duration_ms']
        song_url = item['track']['external_urls']['spotify']
        song_popularity = item['track']['popularity']
        song_added = item['added_at']
        album_id = item['track']['album']['id']
        artist_id = [artist['id'] for artist in item['track']['album']['artists']]
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration,
                        'url': song_url, 'popularity': song_popularity, 'song_added': song_added,
                        'album_id': album_id, 'artist_id': artist_id 
                        }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-mohan'
    Key = 'raw_data/to_processed/'

    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
        
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        album_df = pd.DataFrame(album_list)
        artist_df = pd.DataFrame(artist_list)
        song_df = pd.DataFrame(song_list)

        album_df.drop_duplicates(subset=['album_id'],inplace=True)
        artist_df.drop_duplicates(subset=['artist_id'],inplace=True)
        song_df.drop_duplicates(subset=['song_id'], inplace=True)

        album_df['release_date']=pd.to_datetime(album_df['release_date'])
        song_df['song_added']=pd.to_datetime(song_df['song_added'])

        album_key = 'transformed_data/album_data/albums_transformed_' + str(datetime.now()) + ".csv"
        albums_buffer = StringIO()
        album_df.to_csv(albums_buffer)
        album_content = albums_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key = 'transformed_data/artist_data/artists_transformed_' + str(datetime.now()) + ".csv"
        artists_buffer = StringIO()
        artist_df.to_csv(artists_buffer)
        artist_content = artists_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        song_key = 'transformed_data/songs_data/songs_transformed_' + str(datetime.now()) + ".csv"
        songs_buffer = StringIO()
        song_df.to_csv(songs_buffer)
        song_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()