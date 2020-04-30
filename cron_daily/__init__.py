import azure.functions as func
from urllib.request import urlopen

def main(timer: func.TimerRequest) -> func.HttpResponse:
    resp = urlopen('https://covid-ia-fileoperations.azurewebsites.net/api/ingestion_daily')
    return func.HttpResponse("Daily Cron Called")
