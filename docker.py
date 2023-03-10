import lillorgid.etl.logging
import lillorgid.etl.l.psql.lib
import lillorgid.etl.et.iati.datadump_code4iati
import lillorgid.etl.et.ocds.downloads_odsc
import lillorgid.etl.l.psql.lib
import lillorgid.etl.et.indigo.scraper

import tempfile

lillorgid.etl.logging.python_logging_to_stdout()

lillorgid.etl.logging.logger.info("Docker - Load Lists!")
lillorgid.etl.l.psql.lib.load_lists()

lillorgid.etl.logging.logger.info("Docker - Code4IATI Data Dump!")
worker = lillorgid.etl.et.iati.datadump_code4iati.IATIDataDump(tempfile.mkdtemp(prefix="lillorgiddatadump_code4iati"))
worker.download_data()
worker.extract_zip()
worker.extract_transform()
del worker

lillorgid.etl.logging.logger.info("Docker - OCDS Downloads!")
worker = lillorgid.etl.et.ocds.downloads_odsc.OCDSDataDump(tempfile.mkdtemp(prefix="lillorgiddownloads_odsc"))
worker.download_data()
worker.extract_transform()
del worker

lillorgid.etl.logging.logger.info("Docker - INDIGO!")
worker = lillorgid.etl.et.indigo.scraper.INDIGOScraper(tempfile.mkdtemp(prefix="lillorgidindigo"))
worker.go()
del worker

# TODO Three Sixty Giving

lillorgid.etl.logging.logger.info("Docker - Load all data!")
worker = lillorgid.etl.l.psql.lib.Runner(
    lillorgid.etl.l.psql.lib.Reader(),
    lillorgid.etl.l.psql.lib.Writer()
)
worker.go()
del worker
