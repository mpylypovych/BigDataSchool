import os
from azure.storage.blob import BlobServiceClient

AZURE_STORAGE_CONNECTION_STRING = "removed"

connect_str = AZURE_STORAGE_CONNECTION_STRING

try:
    print("creating client")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    print("created")
    container_name = "homework-2-11-6-11"
    # container_name = "zzz"
    # blob_service_client.delete_container(container_name)

    print("creating container")
    container_client = blob_service_client.create_container(container_name)
    print("created")

    local_path = "../"
    local_file_name = "../yellow_tripdata_2020-01.csv"
    upload_file_path = os.path.join(local_path, local_file_name)

    print("creating blob client")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    print("created")

    print("blob exists: " + str(blob_client.exists()))

    print("Uploading to Azure Storage as blob: " + local_file_name)

    result = None
    # Upload the created file
    with open(upload_file_path, "rb") as data:
        result = blob_client.upload_blob(data)

    print("Uploaded")

    print("blob exists: " + str(blob_client.exists()))


except Exception as ex:
    print('Exception:')
    print(ex)

