import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
import os
from  lillorgid.etl import settings
import lillorgid.etl.logging
import tempfile


class Reader:

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


    def get_latest_data_for_data_standard_and_scraper(self, data_standard, scraper):
        out = set()
        for x in self.blob_container_client.list_blobs(name_starts_with=data_standard+"/"+scraper+"/"):
            out.add(x['name'].split("/")[2])
        out = list(out)
        out.sort()
        return out[-1]

    def get_data_files_in_data_standard_and_scraper_and_dump(self, data_standard, scraper, dump):
        out = set()
        for x in self.blob_container_client.list_blobs(name_starts_with=data_standard+"/"+scraper+"/"+dump+"/"):
            out.add(x['name'].split("/")[-1])
        return list(out)

    def download_data_file_to_temp_file_name(self, data_standard, scraper, dump, filename):
        (handle, out) = tempfile.mkstemp(prefix="load_download")
        os.close(handle)

        with open(file=out, mode="wb") as download_file:
            download_file.write(self.blob_container_client.download_blob(data_standard + "/" + scraper +"/" + dump +"/"+ filename).readall())

        return out

class Runner:

    def __init__(self, reader):
        self.reader = reader


    def go(self):
        for data_standard in self.reader.list_data_standards():
            lillorgid.etl.logging.logger.info("Load Postgres - found data standard "+ data_standard)
            for scraper in self.reader.list_scrapers_for_data_standard(data_standard):
                lillorgid.etl.logging.logger.info("Load Postgres - found data standard " + data_standard + " scraper "+ scraper)
                data_dump_id = self.reader.get_latest_data_for_data_standard_and_scraper(data_standard, scraper)
                lillorgid.etl.logging.logger.info("Load Postgres  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id)
                for data_file in self.reader.get_data_files_in_data_standard_and_scraper_and_dump(data_standard, scraper, data_dump_id):
                    lillorgid.etl.logging.logger.info("Load Postgres  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id + " data file "+ data_file)
                    local_temp_filename = self.reader.download_data_file_to_temp_file_name(data_standard, scraper, data_dump_id, data_file)
                    print(local_temp_filename)



