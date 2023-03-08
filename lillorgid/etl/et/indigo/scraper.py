import os.path
import zipfile
import argparse
import xml.etree.ElementTree
import datetime
import requests

import lillorgid.etl.list_and_id_extractor
import lillorgid.etl.et.lib
import lillorgid.etl.logging

class INDIGOScraper:

    def __init__(self, tmp_dir_name: str):
        self.tmp_dir_name = tmp_dir_name

    def go(self):
        lillorgid.etl.logging.logger.info("INDIGOScraper - Start")
        list_and_id_extractor = lillorgid.etl.list_and_id_extractor.ListAndIdExtractor()
        # All orgs
        r = requests.get("https://golab-indigo-data-store.herokuapp.com/app/api1/organisation")
        r.raise_for_status()
        indigo_org_ids = [ i['id'] for i in r.json().get('organisations', []) if i.get('public')]
        del r
        # For each org
        with lillorgid.etl.et.lib.JSONLinesWriter(self.tmp_dir_name, os.path.join("indigo", "scraper", datetime.datetime.utcnow().isoformat())) as writer:
            for indigo_org_id in indigo_org_ids:
                lillorgid.etl.logging.logger.info("INDIGOScraper Organisation " + indigo_org_id)
                r = requests.get("https://golab-indigo-data-store.herokuapp.com/app/api1/organisation/"+indigo_org_id)
                r.raise_for_status()
                org_ids = set()
                data = r.json().get('organisation', {})
                if data.get('data', {}).get('org-ids'):
                    # Primary?
                    if data.get('data').get('org-ids').get('primary', {}).get('value'):
                        org_ids.add(data.get('data').get('org-ids').get('primary', {}).get('value'))
                    # Secondary?
                    if isinstance(data.get('data').get('org-ids').get('secondary'), list):
                        for data2 in data.get('data').get('org-ids').get('secondary'):
                            if data2.get('organisation_id',{}).get('value'):
                                org_ids.add(data2.get('organisation_id',{}).get('value'))
                # save if any data
                if org_ids:
                    for org_id in list(set(org_ids)):
                        (orgidlist, id, known_list) = list_and_id_extractor.process(org_id)
                        if known_list:
                            meta_data = {
                                'indigo_organisation_id': indigo_org_id
                            }
                            writer.write(orgidlist, id, org_id, url=None, meta_data=meta_data)

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    go_zip_parser = subparsers.add_parser("go")
    go_zip_parser.add_argument("tmp_dir")

    args = parser.parse_args()

    if args.subparser_name == "go":
        worker = INDIGOScraper(args.tmp_dir)
        worker.go()

