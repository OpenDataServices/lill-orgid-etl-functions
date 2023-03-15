import argparse
import lillorgid.etl.l.solr.lib
import lillorgid.etl.logging

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")


    load_list_parser = subparsers.add_parser("loadlist")

    args = parser.parse_args()

    if args.subparser_name == "loadlist":

        lillorgid.etl.l.solr.lib.load_lists()
