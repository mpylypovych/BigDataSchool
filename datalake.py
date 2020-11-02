import os
from azure.storage.filedatalake import DataLakeServiceClient

try:
    storage_account_name = "STORAGE_ACCOUNT_NAME"
    storage_account_key = "KEY"
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)
    container_name = "container_name"

    file_system_client = service_client.create_file_system(file_system=container_name)

    file_system_client.create_directory("folder1/folder2/folder3/folder4/folder5")

    directory_client = file_system_client.get_directory_client("folder1/folder2/folder3/folder4")

    file_client = directory_client.create_file("IndianFoodDatasetCSV.csv")

    local_path = "./"
    local_file_name = "IndianFoodDatasetCSV.csv"
    upload_file_path = os.path.join(local_path, local_file_name)
    local_file = open(upload_file_path, 'rb')

    file_contents = local_file.read()

    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

    file_client.flush_data(len(file_contents))

except Exception as ex:
    print('Exception:')
    print(ex)