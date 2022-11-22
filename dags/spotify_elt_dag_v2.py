import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


SPOTIFY_CLIENT_ID = Variable.get('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = Variable.get('SPOTIFY_CLIENT_SECRET')
GCP_CONN_ID = "google_cloud"
BUCKET = "quantafy-spotify-data"
TEMP_FILE = "/home/airflow/spotify.json"


def get_recently_played(client_id: str, client_secret: str) -> None:
    spotify = spotipy.Spotify(
        auth_manager = SpotifyOAuth( 
            client_id = client_id, 
            client_secret = client_secret, 
            redirect_uri = 'http://localhost:8000',
            scope = "user-read-recently-played",
            cache_path = '.cache',
            open_browser = False
        )
    )

    data = spotify.current_user_recently_played(limit=50)

    with open(TEMP_FILE, 'w') as f:
        json.dump(data, f)

    
default_args = {
    'owner': 'oli',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

now = datetime.utcnow()
filename = f"{int(now.timestamp())}.json"

source_file_path = f"source/{filename}"
raw_file_dir = f"raw/{now.year}/{now.month}/{now.day}/"

with DAG(
    dag_id = "spotify-recently-played-v2",
    default_args = default_args,
    start_date = datetime(2022,11,17,14,0,0),
    schedule= '@hourly'
) as dag:

    query_spotify = PythonOperator(
        task_id = 'query_spotify',
        python_callable = get_recently_played,
        op_kwargs = {
            "client_id": SPOTIFY_CLIENT_ID, 
            "client_secret": SPOTIFY_CLIENT_SECRET
        }
    )

    upload_local_to_source = LocalFilesystemToGCSOperator(
        task_id = "save_local_file_to_source",
        gcp_conn_id = GCP_CONN_ID,
        src = TEMP_FILE,
        dst = source_file_path,
        bucket = BUCKET
    )

    move_source_to_raw = GCSToGCSOperator(
        task_id = "move_source_to_raw",
        gcp_conn_id = GCP_CONN_ID,
        source_bucket = BUCKET,
        source_object = 'source/*.json',
        destination_bucket = BUCKET,
        destination_object = raw_file_dir,
        move_object = True
    )

    query_spotify >>  upload_local_to_source >> move_source_to_raw
