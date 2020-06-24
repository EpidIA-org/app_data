import azure.functions as func
from urllib.request import urlopen

def main(timer: func.TimerRequest) -> None:
    with urlopen('https://epidia-ingestion-functions.azurewebsites.net/api/ingestion_daily') as _:
        _ = True
