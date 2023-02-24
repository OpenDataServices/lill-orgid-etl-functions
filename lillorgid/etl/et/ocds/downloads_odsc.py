import os.path
import zipfile
import argparse
import xml.etree.ElementTree
import datetime
import json
import requests
import csv

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
        zip_file_name = os.path.join(self.tmp_dir_name, "ocdsdata_united_kingdom_contracts_finder_records_csv.zip")
        url = "https://fra1.digitaloceanspaces.com/ocdsdata/united_kingdom_contracts_finder_records/ocdsdata_united_kingdom_contracts_finder_records_csv.zip"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(zip_file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        lillorgid.etl.logging.logger.info("OCDSDataDump - Finish download_data")

    def extract_zip(self):
        lillorgid.etl.logging.logger.info("OCDSDataDump - Start extract_zip")
        zip_file_name = os.path.join(self.tmp_dir_name, "ocdsdata_united_kingdom_contracts_finder_records_csv.zip")
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            zip_ref.extractall(self.tmp_dir_name)
        lillorgid.etl.logging.logger.info("OCDSDataDump - Finish extract_zip")


    def extract_transform(self):
        lillorgid.etl.logging.logger.info("OCDSDataDump - Start extract_transform")
        tmp_data_dir = os.path.join(self.tmp_dir_name, "united_kingdom_contracts_finder_records")
        with lillorgid.etl.et.lib.JSONLinesWriter(self.tmp_dir_name, os.path.join("ocds", "downloads.ocds", datetime.datetime.utcnow().isoformat())) as writer:

            with open(os.path.join(tmp_data_dir, "awards_suppliers.csv")) as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                for row in reader:
                    row_data = {key: value for key, value in zip(headers, row)}
                    if row_data.get('identifier_scheme') and row_data.get('identifier_id'):
                        meta_data = {
                        }
                        writer.write(row_data.get('identifier_scheme'), row_data.get('identifier_id'), meta_data)
        lillorgid.etl.logging.logger.info("OCDSDataDump - Finish extract_transform")

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("tmp_dir")

    extract_zip_parser = subparsers.add_parser("extract_zip")
    extract_zip_parser.add_argument("tmp_dir")

    extract_transform_parser = subparsers.add_parser("extract_transform")
    extract_transform_parser.add_argument("tmp_dir")

    args = parser.parse_args()

    if args.subparser_name == "download":
        worker = OCDSDataDump(args.tmp_dir)
        worker.download_data()

    elif args.subparser_name == "extract_zip":
        worker = OCDSDataDump(args.tmp_dir)
        worker.extract_zip()

    elif args.subparser_name == "extract_transform":
        worker = OCDSDataDump(args.tmp_dir)
        worker.extract_transform()
