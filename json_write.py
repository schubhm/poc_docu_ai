# Imports the Google Cloud client library
from google.cloud import storage
import os
import json

def create_json():
    # Automatically set credentials for your bucket storage
    storage_client = storage.Client()
# json value
    json_file = {
        'CLOUD': 'Google Cloud Platform'
    }
    # Get bucket name from environment variable in app.yaml file
    bucket_name = "docu_test" 
    # os.environ.get('docu_test')
    bucket = storage_client.get_bucket(bucket_name)
    # declare your file name
    blob = bucket.blob('first_text.json')
    # upload json data were we will set content_type as json
    blob.upload_from_string(
        data=json.dumps(json_file),
        content_type='application/json'
        )
    return 'UPLOAD COMPLETE'
def get_json():
    # Automatically set credentials for your bucket storage
    storage_client = storage.Client()
    # Get bucket name from environment variable in app.yaml file
    bucket_name = os.environ.get('docu_test')
    bucket = storage_client.get_bucket(bucket_name)
    # Get the file we want
    blob = bucket.get_blob('first_text.json')
    fileData = json.loads(blob.download_as_string())
    return fileData

create_json()