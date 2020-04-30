from .azure_blob_connector import AzureBlobConnector
from .scrapper_datagouv import DataGouvScrapper, DataGouvSOSMedecinScrapper, DataGouvTestCovidScrapper
from .scrapper_insee import InseeScrapper
from .processor_covidfigures import DailyCovidFiguresProcessor
from .processor_ehpad import DailyEhpadFiguresProcessor
from .processor_insee_death import WeeklyInseeDeathFigureProcessor
