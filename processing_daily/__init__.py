import os
import logging
import azure.functions as func
from datetime import datetime
from _libs import AzureBlobConnector, DailyCovidFiguresProcessor

PROCESSORS_TO_RUN = [DailyCovidFiguresProcessor]


def main(req: func.HttpRequest) -> func.HttpResponse:
    start_time = datetime.now()
    logging.basicConfig(level=logging.INFO)
    # Instantiate Logger
    logger = logging.getLogger("PROCESS DAILY")
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Instantiate Logging")

    # Instantiate Blob Storage Connection
    logger.info(
        f"Init Blob Storage Connection - {os.environ.get('AZURE_STORAGE_CONTAINER_NAME')}"
    )
    abc = AzureBlobConnector(
        credentials=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
        storage_name=os.environ.get("AZURE_STORAGE_CONTAINER_NAME"),
    )

    # Run Processors
    # Fetch files from Storage
    logger.info("Fetch Data From Storage")
    scrappers = [scrapper().fetch(abc) for scrapper in PROCESSORS_TO_RUN]

    # Feed Storage
    logger.info("Process Data")
    scrappers = [scrapper.process() for scrapper in scrappers]

    # Feed Storage
    logger.info("Send Data To Storage")
    scrappers = [scrapper.write(abc) for scrapper in scrappers]

    return func.HttpResponse(f"Process finished. {datetime.now() - start_time}")
