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
        self.connection = psycopg.connect(lillorgid.etl.settings.AZURE_POSTGRES_CONNECTION_STRING, row_factory=psycopg.rows.dict_row)
        self.list_ids = []
        with self.connection.cursor() as cur:
            res = cur.execute("SELECT id FROM list")
            self.list_ids = [r['id'].lower() for r in res]

    def save_data(self, data_standard, scraper, data_dump_id, data):

        with self.connection.cursor() as cur:

            # TODO prepared queries probably faster
            for d in data:
                if d.get('l') and d['l'].lower() in self.list_ids:
                     cur.execute(
                        "INSERT INTO data (list, id, data_standard, scraper, source_id, url, meta) VALUES (%s, %s, %s, %s, %s, %s, %s) "+
                         "ON CONFLICT (list, id, data_standard, source_id) DO UPDATE SET scraper = EXCLUDED.scraper, url = EXCLUDED.url, meta = EXCLUDED.meta",
                        (
                            d['l'].lower(),
                            d['i'],
                            data_standard,
                            scraper,
                            d['sid'],
                            d['u'],
                            json.dumps(d['meta'])
                        )
                     )

            self.connection.commit()

class Runner:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer


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
                    self._process_file(data_standard, scraper, data_dump_id, local_temp_filename)
                    os.remove(local_temp_filename)

    def _process_file(self, data_standard, scraper, data_dump_id, local_temp_filename):
        lillorgid.etl.logging.logger.info(
            "Load Postgres - processing local tmp file " + local_temp_filename)
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
        with psycopg.connect(settings.AZURE_POSTGRES_CONNECTION_STRING) as destination_conn:
            source_cursor = source_conn.cursor()
            with destination_conn.cursor() as destination_cursor:

                res = source_cursor.execute("SELECT * FROM record_lists")
                for row in res.fetchall():

                    destination_cursor.execute(
                        "INSERT INTO list (id) VALUES (%s) ON CONFLICT (id) DO NOTHING",
                        [row['id']]
                    )

                    destination_cursor.execute(
                        "UPDATE list SET "+
                        "field_title_en=%s, "+
                        "field_title_local=%s, "+
                        "field_url=%s, "+
                        "field_description_en=%s, "+
                        "field_coverage=%s, "+
                        #"field_subnationalCoverage=%s, "+
                        "field_structure=%s, "+
                        "field_sector=%s, "+
                        "field_confirmed=%s, "+
                        "field_deprecated=%s, "+
                        #"field_listType=%s, "+
                        #"field_access_availableOnline=%s, "+
                        #"field_access_onlineAccessDetails=%s, "+
                        #"field_access_publicDatabase=%s, "+
                        #"field_access_guidanceOnLocatingIds=%s, "+
                        #"field_access_exampleIdentifiers=%s, "+
                        "field_access_languages=%s, "+
                        "field_data_availability=%s, "+
                        #"field_data_dataAccessDetails=%s, "+
                        "field_data_features=%s, "+
                        #"field_data_licenseStatus=%s, "+
                        #"field_data_licenseDetails=%s, "+
                        "field_meta_source=%s, "+
                        #"field_meta_lastUpdated=%s, "+
                        "field_links_opencorporates=%s, "+
                        "field_links_wikipedia=%s "+
                        #"field_formerPrefixes=%s "+
                        "WHERE id=%s",
                        (
                            row['field_title_en'],
                            row['field_title_local'],
                            row['field_url'],
                            row['field_description_en'],
                            row['field_coverage'],
                            #row['field_subnationalCoverage'],
                            row['field_structure'],
                            row['field_sector'],
                            row['field_confirmed'],
                            row['field_deprecated'],
                            #row['field_listType'],
                            #row['field_access_availableOnline'],
                            #row['field_access_onlineAccessDetails'],
                            #row['field_access_publicDatabase'],
                            #row['field_access_guidanceOnLocatingIds'],
                            #row['field_access_exampleIdentifiers'],
                            row['field_access_languages'],
                            row['field_data_availability'],
                            #row['field_data_dataAccessDetails'],
                            row['field_data_features'],
                            #row['field_data_licenseStatus'],
                            #row['field_data_licenseDetails'],
                            row['field_meta_source'],
                            #row['field_meta_lastUpdated'],
                            row['field_links_opencorporates'],
                            row['field_links_wikipedia'],
                            #row['field_formerPrefixes'],
                            row['id']
                        )
                    )

                    destination_conn.commit()

