from google.cloud import documentai_v1beta2 as documentai
import json
import re

from google.cloud import storage

# Extract shards from the text field
def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return {"start_index": start_index, "end_index": end_index, "response": response}

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def parse_invoice(project_id='temporal-tensor-307222',
        #  input_uri='gs://cloud-samples-data/documentai/invoice.pdf'):
         input_uri='gs://docu_test/AC8BR-05U-2.pdf'):
    """Procsingle document with the Document AI API, including
    text extraction and entity extraction."""

    destination_uri='gs://docu_test/'

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

    # create_json(document)

    # All text extracted from the document
    # print('Document Text: {}'.format(document))

    f = open( 'file.txt', 'w' )
    f.write( 'dict = ' + repr(document) + '\n' )
    f.close()
    
    document_pages = document.pages

    # print(document_pages)

    rows_to_insert = []
    # Read the text recognition output from the processor
    print("The document contains the following paragraphs:")
    for page in document_pages:
        paragraphs = page.paragraphs
        for paragraph in paragraphs:
            paragraph_text = get_text(paragraph.layout, document)
            # print(f"Paragraph text: {paragraph_text}")
            print(paragraph_text)
            y = json.dumps(paragraph_text)

            # the result is a JSON string:
            print(y)

            rows_to_insert.append(y)
        print(rows_to_insert)
        with open('data.txt', 'w') as outfile:
            json.dump(rows_to_insert, outfile)
        upload_blob("docu_test","data.txt","data.json")

    from google.cloud import bigquery

    # # Construct a BigQuery client object.
    client = bigquery.Client()

    # # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "temporal-tensor-307222.docu_test.printer_tab"

    # # Set the encryption key to use for the destination.
    # # TODO: Replace this key with a key you have created in KMS.
    # # kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
    # #     "cloud-samples-tests", "us", "test", "test"
    # # )
    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    uri = "gs://docu_test/data.json"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
     )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


    # document1 =documentai.types.Document.from_json(document)

    if (1==2):

        # Results are written to GCS. Use a regex to find
        # output files
        match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
        output_bucket = match.group(1)
        prefix = match.group(2)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(output_bucket)
        blob_list = list(bucket.list_blobs(prefix=prefix))
        print("Output files:")

        for i, blob in enumerate(blob_list):
            # If JSON file, download the contents of this blob as a bytes object.
            if ".json" in blob.name:
                blob_as_bytes = blob.download_as_bytes()

                document = documentai.types.Document.from_json(blob_as_bytes)
                print(f"Fetched file {i + 1}")

                # For a full list of Document object attributes, please reference this page:
                # https://cloud.google.com/document-ai/docs/reference/rpc/google.cloud.documentai.v1beta3#document

                # Read the text recognition output from the processor
                for page in document.pages:
                    for form_field in page.form_fields:
                        field_name = get_text(form_field.field_name, document)
                        field_value = get_text(form_field.field_value, document)
                        print("Extracted key value pair:")
                        print(f"\t{field_name}, {field_value}")
                    for paragraph in document.pages:
                        paragraph_text = get_text(paragraph.layout, document)
                        # print(f"Paragraph text:\n{paragraph_text}")
            else:
                print(f"Skipping non-supported file type {blob.name}")


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
