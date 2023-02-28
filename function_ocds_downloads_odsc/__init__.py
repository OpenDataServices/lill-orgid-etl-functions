import lillorgid.etl.et.ocds.downloads_odsc
import tempfile
import lillorgid.etl.logging


lillorgid.etl.logging.log_to_azure()

def main(context):
    lillorgid.etl.logging.logger.info("Function function_ocds_downloads_odsc called")
    worker = lillorgid.etl.et.ocds.downloads_odsc.OCDSDataDump(tempfile.mkdtemp(prefix="lillorgidet"))
    worker.download_data()
    worker.extract_transform()
    return "IATI datadump_code4iati done"
