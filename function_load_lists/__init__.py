import lillorgid.etl.logging
import lillorgid.etl.l.solr.lib

lillorgid.etl.logging.log_to_azure()

def main(context):
    lillorgid.etl.logging.logger.info("Function function_load_lists called")
    lillorgid.etl.l.solr.lib.load_lists()
    return "function_load_lists done"
