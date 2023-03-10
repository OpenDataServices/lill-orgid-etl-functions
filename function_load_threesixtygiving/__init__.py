import lillorgid.etl.logging
import lillorgid.etl.l.psql.lib

lillorgid.etl.logging.log_to_azure()


def main(context):
    lillorgid.etl.logging.logger.info("Function function_load_threesixtygiving called")

    worker = lillorgid.etl.l.psql.lib.Runner(
        lillorgid.etl.l.psql.lib.Reader(),
        lillorgid.etl.l.psql.lib.Writer()
    )
    worker.go_data_standard('threesixtygiving')

    return "function_load_threesixtygiving done"
