import lillorgid.etl.et.iati.datadump_code4iati
import tempfile
import lillorgid.etl.logging

lillorgid.etl.logging.log_to_azure()

def main(context):
    lillorgid.etl.logging.logger.info("Function function_iati_datadump_code4iati called")
    worker = lillorgid.etl.et.iati.datadump_code4iati.IATIDataDump(tempfile.mkdtemp(prefix="lillorgidet"))
    worker.download_data()
    worker.extract_zip()
    worker.extract_transform()
    return "IATI datadump_code4iati done"
