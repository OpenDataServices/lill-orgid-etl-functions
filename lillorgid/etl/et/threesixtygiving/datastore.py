import os.path
import zipfile
import argparse
import xml.etree.ElementTree
import datetime
import psycopg

import lillorgid.etl.list_and_id_extractor
import lillorgid.etl.et.lib
import lillorgid.etl.logging
from  lillorgid.etl import settings

class ThreeSixtyGivingDataStore:

    def __init__(self, tmp_dir_name: str):
        self.tmp_dir_name = tmp_dir_name

    def go(self):
        lillorgid.etl.logging.logger.info("ThreeSixtyGivingDataStore - Start")
        list_and_id_extractor = lillorgid.etl.list_and_id_extractor.ListAndIdExtractor()

        with lillorgid.etl.et.lib.JSONLinesWriter(self.tmp_dir_name, os.path.join("threesixtygiving", "datastore", datetime.datetime.utcnow().isoformat())) as writer:
            with psycopg.connect(settings.THREESIXTYGIVING_DATASTORE_POSTGRES_CONNECTION_STRING, row_factory=psycopg.rows.dict_row) as destination_conn:
                with destination_conn.cursor() as destination_cursor:

                    res = destination_cursor.execute("select * from view_latest_grant where (data->'recipientOrganization'->0->'id')::text = '\"GB-CHC-1044940\"'")
                    for row in res.fetchall():
                        for recipient_organization in row['data'].get('recipientOrganization', []):
                            org_id = recipient_organization.get('id')
                            if org_id:
                                (orgidlist, id, known_list) = list_and_id_extractor.process(org_id)
                                if known_list:
                                    meta_data = {
                                        'grant_id': row['grant_id']
                                    }
                                    writer.write(orgidlist, id, row['grant_id'], url=None, meta_data=meta_data)

                    # Funders - could do in block above or look at  db_funder table
                    # TODO

        lillorgid.etl.logging.logger.info("ThreeSixtyGivingDataStore - End")

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    go_parser = subparsers.add_parser("go")
    go_parser.add_argument("tmp_dir")

    args = parser.parse_args()

    if args.subparser_name == "go":
        worker = ThreeSixtyGivingDataStore(args.tmp_dir)
        worker.go()

