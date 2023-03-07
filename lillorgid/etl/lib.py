import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
import os
from  lillorgid.etl import settings
import lillorgid.etl.logging
import tempfile
import sqlite3
import requests
import psycopg

class StorageDeleter:

    def __init__(self):
        account_url = "https://"+ settings.AZURE_STORAGE_ACCOUNT_NAME +".blob.core.windows.net"
        if settings.AZURE_STORAGE_CONNECTION_STRING:
            self.blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
        else:
            self.blob_service_client = BlobServiceClient(account_url, credential=DefaultAzureCredential())
        self.blob_container_client = self.blob_service_client.get_container_client(settings.AZURE_STORAGE_CONTAINER_NAME)

    def list_data_standards(self):
        out = set()
        for x in self.blob_container_client.list_blobs():
            out.add(x['name'].split("/")[0])
        return list(out)

    def list_scrapers_for_data_standard(self, data_standard):
        out = set()
        for x in self.blob_container_client.list_blobs(name_starts_with=data_standard+"/"):
            out.add(x['name'].split("/")[1])
        return list(out)

    def get_old_data_for_data_standard_and_scraper(self, data_standard, scraper, min_to_leave=5):
        out = set()
        for x in self.blob_container_client.list_blobs(name_starts_with=data_standard+"/"+scraper+"/"):
            out.add(x['name'].split("/")[2])
        out = list(out)
        if len(out) < min_to_leave:
            return []
        out.sort()
        return out[:-min_to_leave]
        
    def get_data_files_in_data_standard_and_scraper_and_dump(self, data_standard, scraper, dump):
        out = set()
        for x in self.blob_container_client.list_blobs(name_starts_with=data_standard+"/"+scraper+"/"+dump+"/"):
            out.add(x['name'].split("/")[-1])
        return list(out)

    def delete(self, data_standard, scraper, dump, filename):

        blob_client = self.blob_service_client.get_blob_client(container=settings.AZURE_STORAGE_CONTAINER_NAME,
                                                               blob=data_standard + "/" + scraper +"/" + dump +"/"+ filename)
        blob_client.delete_blob(delete_snapshots="include")

    def go(self, actually_delete=False):
        for data_standard in self.list_data_standards():
            lillorgid.etl.logging.logger.info("Data Deleter -found data standard "+ data_standard)
            for scraper in self.list_scrapers_for_data_standard(data_standard):
                lillorgid.etl.logging.logger.info("Data Deleter -found data standard " + data_standard + " scraper "+ scraper)
                for data_dump_id in self.get_old_data_for_data_standard_and_scraper(data_standard, scraper):
                    lillorgid.etl.logging.logger.info("Load Postgres  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id)
                    for data_file in self.get_data_files_in_data_standard_and_scraper_and_dump(data_standard, scraper, data_dump_id):
                        lillorgid.etl.logging.logger.info("Load Postgres  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id + " data file "+ data_file)
                        if actually_delete:
                            self.delete(data_standard, scraper, data_dump_id, data_file)
