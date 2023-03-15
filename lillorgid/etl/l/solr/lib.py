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
            r = requests.post(url, json=post_data, headers=headers)
            #print(r.json())
            r.raise_for_status()
