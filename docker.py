import lillorgid.etl.logging
import lillorgid.etl.et.iati.datadump_code4iati
import lillorgid.etl.l.solr.lib
import lillorgid.etl.et.indigo.scraper
import lillorgid.etl.et.threesixtygiving.datastore
import os
import tempfile

lillorgid.etl.logging.python_logging_to_stdout()

if os.getenv("RUN_LOAD_LISTS"):
    try:
        lillorgid.etl.logging.logger.info("Docker - Load Lists!")
        lillorgid.etl.l.solr.lib.load_lists()
    except Exception as e:
        lillorgid.etl.logging.logger.info("ERROR - " + str(e))

if os.getenv("RUN_CODE4IATIDATADUMP"):
    try:
        lillorgid.etl.logging.logger.info("Docker - Code4IATI Data Dump!")
        worker = lillorgid.etl.et.iati.datadump_code4iati.IATIDataDump(tempfile.mkdtemp(prefix="lillorgiddatadump_code4iati"))
        worker.download_data()
        worker.extract_zip()
        worker.extract_transform()
        del worker
    except Exception as e:
        lillorgid.etl.logging.logger.info("ERROR - " + str(e))

if os.getenv("RUN_INDIGO"):
    try:
        lillorgid.etl.logging.logger.info("Docker - INDIGO!")
        worker = lillorgid.etl.et.indigo.scraper.INDIGOScraper(tempfile.mkdtemp(prefix="lillorgidindigo"))
        worker.go()
        del worker
    except Exception as e:
        lillorgid.etl.logging.logger.info("ERROR - " + str(e))

if os.getenv("RUN_360G"):
    try:
        lillorgid.etl.logging.logger.info("Docker - Three Sixty Giving!")
        worker = lillorgid.etl.et.threesixtygiving.datastore.ThreeSixtyGivingDataStore(tempfile.mkdtemp(prefix="lillorgidthreesixtygiving"))
        worker.go()
        del worker
    except Exception as e:
        lillorgid.etl.logging.logger.info("ERROR - " + str(e))

if os.getenv("RUN_LOAD_DATA"):
    try:
        lillorgid.etl.logging.logger.info("Docker - Load all data!")
        worker = lillorgid.etl.l.solr.lib.Runner(
            lillorgid.etl.l.solr.lib.Reader(),
            lillorgid.etl.l.solr.lib.Writer()
        )
        worker.go()
        del worker
    except Exception as e:
        lillorgid.etl.logging.logger.info("ERROR - " + str(e))

