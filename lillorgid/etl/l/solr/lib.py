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

class Writer:

    def __init__(self):
        pass

    def save_data(self, data_standard, scraper, data_dump_id, data):

        headers = {'Content-type': 'application/json'}
        url = settings.SOLR_URL + "/" + settings.SOLR_DATA_CORE + "/update"

        for data_row in data:

            post_data = {
                "add": {
                    'doc': {
                        'id': data_standard + "--" + data_row['l'] + "--" + data_row['i'] + "--" + data_row['sid'],
                        'datastandard_s': data_standard,
                        'scraper_s': scraper,
                        'list_s': data_row['l'],
                        'id_s': data_row['i'],
                        'name_s': data_row.get('n'),
                        'url_s': data_row.get('u'),
                    },
                    "commitWithin": 10000
                }
            }


            print(data_row)
            for k, v in data_row.get('meta',{}).items():
                post_data['add']['doc']['meta_'+k+"_s"] = v

            r = requests.post(url, json=post_data, headers=headers, auth=requests.auth.HTTPBasicAuth(settings.SOLR_USERNAME, settings.SOLR_PASSWORD))
            print(r.json())
            r.raise_for_status()


class Runner:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer


    def go(self):
        for data_standard in self.reader.list_data_standards():
            lillorgid.etl.logging.logger.info("Load Solr - found data standard "+ data_standard)
            self.go_data_standard(data_standard)

    def go_data_standard(self, data_standard):
        for scraper in self.reader.list_scrapers_for_data_standard(data_standard):
            lillorgid.etl.logging.logger.info("Load Solr - found data standard " + data_standard + " scraper "+ scraper)
            data_dump_id = self.reader.get_latest_data_for_data_standard_and_scraper(data_standard, scraper)
            lillorgid.etl.logging.logger.info("Load Solr  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id)
            for data_file in self.reader.get_data_files_in_data_standard_and_scraper_and_dump(data_standard, scraper, data_dump_id):
                lillorgid.etl.logging.logger.info("Load Solr  - found data standard " + data_standard + " scraper "+ scraper + " data dump "+ data_dump_id + " data file "+ data_file)
                local_temp_filename = self.reader.download_data_file_to_temp_file_name(data_standard, scraper, data_dump_id, data_file)
                self._process_file(data_standard, scraper, data_dump_id, local_temp_filename)
                os.remove(local_temp_filename)

    def _process_file(self, data_standard, scraper, data_dump_id, local_temp_filename):
        lillorgid.etl.logging.logger.info(
            "Load Solr - processing local tmp file " + local_temp_filename)
        data = []
        with open(local_temp_filename) as f:
            for line in f:
                json_data = json.loads(line)
                data.append(json_data)
        self.writer.save_data(data_standard, scraper, data_dump_id, data)




def load_lists():
    # Get DB
    tempdir = tempfile.mkdtemp(prefix="lillorgidet")
    tempfilename = os.path.join(tempdir, "db.sqlite")
    url = "https://org-id-register.netlify.app/database.sqlite"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(tempfilename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Let's go

    with sqlite3.connect(tempfilename) as source_conn:
        source_conn.row_factory = sqlite3.Row

        source_cursor = source_conn.cursor()

        res = source_cursor.execute("SELECT * FROM record_lists")

        headers = {'Content-type': 'application/json'}
        url = settings.SOLR_URL + "/" + settings.SOLR_LISTS_CORE + "/update"

        for row in res.fetchall():

            post_data= {
                "add": {
                    'doc': {
                        'id': row['id'],
                        'title_en_s': row['field_title_en'],
                        "title_local_s": row['field_title_local'],
                        "url_s": row['field_url'],
                        "description_en_s": row['field_description_en'],
                        "coverage_s": row['field_coverage'],
                        "subnationalCoverage_s": row['field_subnationalCoverage'],
                        "structure_s": row['field_structure'],
                        "sector_s": row['field_sector'],
                        "confirmed_s": row['field_confirmed'],
                        "deprecated_s": row['field_deprecated'],
                        "listType_s": row['field_listType'],
                        "access_availableOnline_s": row['field_access_availableOnline'],
                        "access_onlineAccessDetails_s": row['field_access_onlineAccessDetails'],
                        "access_publicDatabase_s": row['field_access_publicDatabase'],
                        "access_guidanceOnLocatingIds_s": row['field_access_guidanceOnLocatingIds'],
                        "access_exampleIdentifiers_s": row['field_access_exampleIdentifiers'],
                        "access_languages_s": row['field_access_languages'],
                        "data_availability_s": row['field_data_availability'],
                        "data_dataAccessDetails_s": row['field_data_dataAccessDetails'],
                        "data_features_s": row['field_data_features'],
                        "data_licenseStatus_s": row['field_data_licenseStatus'],
                        "data_licenseDetails_s": row['field_data_licenseDetails'],
                        "meta_source_s": row['field_meta_source'],
                        "meta_lastUpdated_s": row['field_meta_lastUpdated'],
                        "links_opencorporates_s": row['field_links_opencorporates'],
                        "links_wikipedia_s": row['field_links_wikipedia'],
                        "formerPrefixes_s": row['field_formerPrefixes']
                    },
                    "commitWithin": 10000
                }
            }
            r = requests.post(url, json=post_data, headers=headers, auth=requests.auth.HTTPBasicAuth(settings.SOLR_USERNAME, settings.SOLR_PASSWORD))
            #print(r.json())
            r.raise_for_status()
