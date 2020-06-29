import azure.functions as func
import logging
#from urllib.request import urlopen
import requests

def main(timer: func.TimerRequest) -> None:
    logging.basicConfig(level=logging.INFO)
    # Instantiate Logger
    logger = logging.getLogger("INGESTION DAILY")
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # with urlopen('https://epidia-ingestion-functions.azurewebsites.net/api/ingestion_daily') as _:
    #     _ = True
    r = requests.get('https://epidia-ingestion-functions.azurewebsites.net/api/ingestion_daily')
    if r.status_code >= 200 and r.status_code < 300:
        logger.info("CRON DAILY SUCCESS!")
    else:
        logger.error("CRON DAILY ERROR!")
        logger.error(r.json())
