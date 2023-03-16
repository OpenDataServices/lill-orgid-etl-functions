import lillorgid.etl.logging
import lillorgid.etl.l.solr.lib

lillorgid.etl.logging.log_to_azure()


def main(context):
    lillorgid.etl.logging.logger.info("Function function_load_all_data called")

    worker = lillorgid.etl.l.solr.lib.Runner(
        lillorgid.etl.l.solr.lib.Reader(),
        lillorgid.etl.l.solr.lib.Writer()
    )
    worker.go()

    return "function_load_all_data done"
