import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
import os
from  lillorgid.etl import settings


class JSONLinesWriter:

    def __init__(self, tmp_directory, blob_directory):
        self.tmp_directory = tmp_directory
        self.blob_directory = blob_directory
        self.fp = None
        self.file_counter = 1
        self.pagesize = 10000
        self.data_counter = 0


    def __enter__(self):
        os.makedirs(os.path.join(self.tmp_directory, self.blob_directory))
        self.fp = open(os.path.join(self.tmp_directory, self.blob_directory, "data."+ "{:07d}".format(self.file_counter) +".jsonlines"), "w")

        if settings.AZURE_STORAGE_UPLOAD:
            # Create the BlobServiceClient object
            account_url = "https://"+ settings.AZURE_STORAGE_ACCOUNT_NAME +".blob.core.windows.net"
            if settings.AZURE_STORAGE_CONNECTION_STRING:
                self.blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
            else:
                self.blob_service_client = BlobServiceClient(account_url, credential=DefaultAzureCredential())

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_and_upload_current_file()

    def write(self, list, id, source_id, url=None, meta_data={}):
        d = {"l":list, "i":id, "sid": source_id, "u":url, "meta": meta_data}
        self.fp.write(json.dumps(d)+"\n")
        self.data_counter += 1
        if self.data_counter > self.pagesize:
            self._close_and_upload_current_file()
            self.data_counter = 0
            self.file_counter += 1
            self.fp = open(os.path.join(self.tmp_directory, self.blob_directory, "data."+ "{:07d}".format(self.file_counter) +".jsonlines"), "w")


    def _close_and_upload_current_file(self):
        self.fp.close()
        if settings.AZURE_STORAGE_UPLOAD:
            blob_client = self.blob_service_client.get_blob_client(container=settings.AZURE_STORAGE_CONTAINER_NAME, blob=os.path.join(self.blob_directory, "data."+ "{:07d}".format(self.file_counter) +".jsonlines"))
            with open(file=os.path.join(self.tmp_directory, self.blob_directory, "data."+ "{:07d}".format(self.file_counter) +".jsonlines"), mode="rb") as data:
                blob_client.upload_blob(data)
