import os
import logging
import azure.functions as func
from datetime import datetime
from urllib.request import urlopen
from ._libs import AzureBlobConnector, InseeScrapper

SCRAPPERS_TO_RUN = [InseeScrapper]


def main(req: func.HttpRequest) -> func.HttpResponse:
    start_time = datetime.now()
    logging.basicConfig(level=logging.INFO)
    # Instantiate Logger
    logger = logging.getLogger("INGESTION WEEKLY")
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

    # Run Scrappers
    logger.info("Scraping Data")
    scrappers = [scrapper().fetch() for scrapper in SCRAPPERS_TO_RUN]

    # Feed Storage
    logger.info("Scraping Data")
    scrappers = [scrapper.write(abc) for scrapper in scrappers]

    resp = urlopen('https://covid-ia-fileoperations.azurewebsites.net/api/processing_weekly')

    return func.HttpResponse(f"Process finished. {datetime.now() - start_time}")
