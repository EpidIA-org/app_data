import azure.functions as func
from urllib.request import urlopen

def main(timer: func.TimerRequest) -> None:
    resp = urlopen('https://covid-ia-fileoperations.azurewebsites.net/api/ingestion_weekly')
