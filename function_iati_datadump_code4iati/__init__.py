import lillorgid.et.iati.datadump_code4iati
import tempfile
import lillorgid.et.logging

lillorgid.et.logging.log_to_azure()

def main(context):
    lillorgid.et.logging.logger.info("Function function_iati_datadump_code4iati called")
    worker = lillorgid.et.iati.datadump_code4iati.IATIDataDump(tempfile.mkdtemp(prefix="lillorgidet"))
    worker.download_data()
    worker.extract_zip()
    worker.extract_transform()
    return "IATI datadump_code4iati done"
