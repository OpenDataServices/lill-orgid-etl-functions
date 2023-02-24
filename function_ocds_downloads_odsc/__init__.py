import lillorgid.et.ocds.downloads_odsc
import tempfile
import lillorgid.et.logging


lillorgid.et.logging.log_to_azure()

def main(context):
    lillorgid.et.logging.logger.info("Function function_ocds_downloads_odsc called")
    worker = lillorgid.et.ocds.downloads_odsc.OCDSDataDump(tempfile.mkdtemp(prefix="lillorgidet"))
    worker.download_data()
    worker.extract_zip()
    worker.extract_transform()
    return "IATI datadump_code4iati done"
