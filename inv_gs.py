from google.cloud import documentai_v1beta2 as documentai

# Imports the Google Cloud client library
from google.cloud import storage
import os
import json

def create_json(json_file1):
    # Automatically set credentials for your bucket storage
    storage_client = storage.Client()
# json value
    json_file = {
        'CLOUD': 'Google Cloud Platform'
    }
    json_file = json_file1
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

def parse_invoice(project_id='temporal-tensor-307222',
         input_uri='gs://cloud-samples-data/documentai/invoice.pdf'):
        #  input_uri='gs://docu_test/AC8BR-05U.PDF'):
    """Procsingle document with the Document AI API, including
    text extraction and entity extraction."""

    client = documentai.DocumentUnderstandingServiceClient()

    gcs_source = documentai.types.GcsSource(uri=input_uri)

    # mime_type can be application/pdf, image/tiff,
    # and image/gif, or application/json
    input_config = documentai.types.InputConfig(
        gcs_source=gcs_source, mime_type='application/pdf')

    # Location can be 'us' or 'eu'
    parent = 'projects/{}/locations/us'.format(project_id)
    request = documentai.types.ProcessDocumentRequest(
        parent=parent,
        input_config=input_config)

    document = client.process_document(request=request)

    print(type(document.content))

    # create_json(json.dump(document))

    # All text extracted from the document
    print('Document Text: {}'.format(document.content))

#     bq --location=LOCATION load \
# --source_format=FORMAT \
# DATASET.TABLE \
# PATH_TO_SOURCE \
# SCHEMA

# storage_client = storage.Client()

# storage_client = storage.Client.from_service_account_json('service_account.json')

# json_file = {
#         'CLOUD': 'Google Cloud Platform'
#   }

parse_invoice()
