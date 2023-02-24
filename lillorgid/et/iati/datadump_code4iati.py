import os.path
import zipfile
import argparse
import xml.etree.ElementTree
import datetime
import requests

import lillorgid.et.lib
import lillorgid.et.logging

class IATIDataDump:



    def __init__(self, tmp_dir_name: str):
        self.tmp_dir_name = tmp_dir_name

    def download_data(self):
        lillorgid.et.logging.logger.info("IATIDataDump - Start download_data")
        zip_file_name = os.path.join(self.tmp_dir_name, "iati-data-main.zip")
        url = "https://gitlab.com/codeforIATI/iati-data/-/archive/main/iati-data-main.zip"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(zip_file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        lillorgid.et.logging.logger.info("IATIDataDump - Finished download_data")


    def extract_zip(self):
        lillorgid.et.logging.logger.info("IATIDataDump - Start extract_zip")
        zip_file_name = os.path.join(self.tmp_dir_name, "iati-data-main.zip")
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            zip_ref.extractall(self.tmp_dir_name)
        lillorgid.et.logging.logger.info("IATIDataDump - Finished extract_zip")


    def extract_transform(self):
        lillorgid.et.logging.logger.info("IATIDataDump - Start extract_transform")
        tmp_data_dir = os.path.join(self.tmp_dir_name, "iati-data-main", "data")
        with lillorgid.et.lib.JSONLinesWriter(self.tmp_dir_name, os.path.join("iati", "datadump.code4iati", datetime.datetime.utcnow().isoformat())) as writer:
            for dir_name in os.listdir(tmp_data_dir):
                for file_name in os.listdir(os.path.join(tmp_data_dir, dir_name)):
                    lillorgid.et.logging.logger.info("IATIDataDump - extract_transform - Starting Directory: "+ dir_name + " File: " + file_name)
                    tree = xml.etree.ElementTree.parse(os.path.join(tmp_data_dir, dir_name, file_name))
                    root = tree.getroot()
                    if root.tag == 'iati-activities':
                        for activity in root.findall('iati-activity'):
                            # set up vars
                            iati_identifier = None
                            org_references = []
                            # get info
                            iati_identifier = activity.find('iati-identifier').text.strip()
                            for field_name in ['reporting-org','participating-org']:
                                for org_reference_xml in activity.findall(field_name):
                                    org_references.append(org_reference_xml.attrib.get('ref'))
                            # TODO look up all the other places org ID's could be and put in!
                            # output info
                            for org_reference in org_references:
                                meta_data = {
                                    'activity-id': iati_identifier,
                                    'dir': dir_name,
                                    'file': file_name,
                                }
                                # TODO split list and id
                                writer.write(org_reference, org_reference, meta_data)


                    break
                break
        lillorgid.et.logging.logger.info("IATIDataDump - Finished extract_transform")


if __name__ == "__main__":
    lillorgid.et.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    extract_zip_parser = subparsers.add_parser("extract_zip")
    extract_zip_parser.add_argument("tmp_dir")

    extract_transform_parser = subparsers.add_parser("extract_transform")
    extract_transform_parser.add_argument("tmp_dir")

    args = parser.parse_args()

    if args.subparser_name == "extract_zip":
        worker = IATIDataDump(args.tmp_dir)
        worker.extract_zip()

    elif args.subparser_name == "extract_transform":
        worker = IATIDataDump(args.tmp_dir)
        worker.extract_transform()
