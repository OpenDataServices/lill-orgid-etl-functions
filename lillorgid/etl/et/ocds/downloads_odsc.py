import os.path
import zipfile
import argparse
import xml.etree.ElementTree
import datetime
import json
import requests
import csv
import sqlite3

import lillorgid.etl.et.lib
import lillorgid.etl.logging

class OCDSDataDump:
    """
    From https://ocds-downloads.opendata.coop/
    """

    def __init__(self, tmp_dir_name: str):
        self.tmp_dir_name = tmp_dir_name

    def download_data(self):
        lillorgid.etl.logging.logger.info("OCDSDataDump - Start download_data")
        db_file_name = os.path.join(self.tmp_dir_name, "ocdsdata_united_kingdom_contracts_finder_records_csv.sqlite")
        url = "https://fra1.digitaloceanspaces.com/ocdsdata/united_kingdom_contracts_finder_records/ocdsdata_united_kingdom_contracts_finder_records.sqlite"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(db_file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        lillorgid.etl.logging.logger.info("OCDSDataDump - Finish download_data")

    def extract_transform(self):
        lillorgid.etl.logging.logger.info("OCDSDataDump - Start extract_transform")
        db_file_name = os.path.join(self.tmp_dir_name, "ocdsdata_united_kingdom_contracts_finder_records_csv.sqlite")
        with sqlite3.connect(db_file_name) as source_conn:
            source_conn.row_factory = sqlite3.Row
            source_cursor = source_conn.cursor()
            with lillorgid.etl.et.lib.JSONLinesWriter(self.tmp_dir_name, os.path.join("ocds", "downloads.ocds", datetime.datetime.utcnow().isoformat())) as writer:

                # Awards Suppliers
                res = source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='awards_suppliers';")
                if res.fetchone():
                    res = source_cursor.execute(
                        "SELECT awards_suppliers.identifier_scheme, awards_suppliers.identifier_id, release.ocid "+
                        "FROM awards_suppliers "+
                        "JOIN release ON awards_suppliers._link_release = release._link "+
                        "WHERE awards_suppliers.identifier_scheme IS NOT NULL AND awards_suppliers.identifier_id IS NOT NULL  "+
                        "AND awards_suppliers.identifier_scheme != '' AND awards_suppliers.identifier_id != ''  "
                    )
                    for row in res.fetchall():
                        meta_data = {
                            }
                        writer.write(row['identifier_scheme'], row['identifier_id'], "release-" + row['ocid'], url=None, meta_data=meta_data)

        lillorgid.etl.logging.logger.info("OCDSDataDump - Finish extract_transform")

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("tmp_dir")

    extract_transform_parser = subparsers.add_parser("extract_transform")
    extract_transform_parser.add_argument("tmp_dir")

    args = parser.parse_args()

    if args.subparser_name == "download":
        worker = OCDSDataDump(args.tmp_dir)
        worker.download_data()


    elif args.subparser_name == "extract_transform":
        worker = OCDSDataDump(args.tmp_dir)
        worker.extract_transform()
