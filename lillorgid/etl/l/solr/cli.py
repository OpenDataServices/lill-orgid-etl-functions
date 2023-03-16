import argparse
import lillorgid.etl.l.solr.lib
import lillorgid.etl.logging

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")


    load_list_parser = subparsers.add_parser("loadlist")

    load_parser = subparsers.add_parser("load")

    args = parser.parse_args()

    if args.subparser_name == "loadlist":

        lillorgid.etl.l.solr.lib.load_lists()

    elif args.subparser_name == "load":


        worker = lillorgid.etl.l.solr.lib.Runner(
            lillorgid.etl.l.solr.lib.Reader(),
            lillorgid.etl.l.solr.lib.Writer()
        )
        worker.go()
