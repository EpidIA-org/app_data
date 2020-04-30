import logging
from bs4 import BeautifulSoup
from urllib.request import urlopen
from .azure_blob_connector import AzureBlobConnector


class Scrapper:
    _SCRAPPER_NAME = "MetaScrapper"
    URL = "http://DUMMY_URL.org"

    def __init__(self):
        self._request = urlopen(self.URL)
        self._soup = None
        self.data_to_retrieve = []
        self.is_logger_generated = False
        self.is_logger_generated = self._get_logger()

    def __call__(self):
        return self.fetch().write()

    def _get_logger(self):
        self._logger = logging.getLogger(self._SCRAPPER_NAME)
        self._handler = logging.StreamHandler()
        self._handler.setLevel(logging.INFO)
        self._formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self._handler.setFormatter(self._formatter)
        self._logger.addHandler(self._handler)
        return True

    def fetch(self):
        self._logger.info("1/2 Fetching data")
        self._logger.info("1/2 Fetching data - Parsing HTML")
        self._soup = BeautifulSoup(self._request.read(), "html.parser")
        self._fetch()
        self._logger.info("1/2 Fetching data - DONE - Data Fetch")
        return self

    def write(self, conn: AzureBlobConnector):
        self._logger.info("2/2 Writing data")
        self._write(conn=conn)
        self._logger.info("2/2 Writing data - DONE - Files Sent")
        return self

    def _fetch(self):
        raise NotImplementedError

    def _write(self, conn: AzureBlobConnector):
        self._logger.info("2/2 Writing data - Sending Files")
        [conn.send_data(f["data"], f["name"]) for f in self.data_to_retrieve]
