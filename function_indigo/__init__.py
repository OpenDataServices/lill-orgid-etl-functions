import lillorgid.etl.et.indigo.scraper
import tempfile
import lillorgid.etl.logging


lillorgid.etl.logging.log_to_azure()

def main(context):
    lillorgid.etl.logging.logger.info("Function function_indigo called")
    worker = lillorgid.etl.et.indigo.scraper.INDIGOScraper(tempfile.mkdtemp(prefix="lillorgidet"))
    worker.go()
    return "INDIGO done"
