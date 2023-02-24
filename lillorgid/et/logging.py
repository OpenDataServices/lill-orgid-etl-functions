import logging
import sys
from opencensus.ext.azure.log_exporter import AzureLogHandler


logger = logging.getLogger("lill-orgid-extract-and-transform")


def python_logging_to_stdout():
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def log_to_azure():
    # You can also instantiate the exporter directly if you have the environment variable
    # `APPLICATIONINSIGHTS_CONNECTION_STRING` configured
    logger.addHandler(AzureLogHandler())

