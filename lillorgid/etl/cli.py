import argparse
import lillorgid.etl.lib
import lillorgid.etl.logging

if __name__ == "__main__":
    lillorgid.etl.logging.python_logging_to_stdout()

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="subparser_name")

    deletedata_parser = subparsers.add_parser("deletedata")

    args = parser.parse_args()

    if args.subparser_name == "deletedata":

        worker = lillorgid.etl.lib.StorageDeleter()
        worker.go(actually_delete=False)
