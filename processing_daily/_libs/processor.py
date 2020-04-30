import logging
import re
from _libs.azure_blob_connector import AzureBlobConnector


class Processor:
    REQUIRED_FILE = []
    _PROCESSOR_NAME = "Processor"

    def __init__(self):
        self.source_files = self.REQUIRED_FILE
        self.genuine_source_date = False
        self.is_logger_generated = False
        self.is_logger_generated = self._get_logger()
        self.date = None
        self.export_name = None
        self.export_data = None

    def __call__(self):
        return self.fetch().process().write()

    def _get_logger(self):
        self._logger = logging.getLogger(self._PROCESSOR_NAME)
        self._handler = logging.StreamHandler()
        self._handler.setLevel(logging.INFO)
        self._formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self._handler.setFormatter(self._formatter)
        self._logger.addHandler(self._handler)
        return True

    def _last_version(self, conn: AzureBlobConnector):
        self._logger.info("1/3 Fetching data - Checking Source File Date")
        self.source_files = [
            {
                **f,
                **{
                    "date": re.search('(\d{4}-\d{2}-\d{2})', conn.get_last_filename_version(f["stem"])).group(1),
                    "file": conn.get_last_filename_version(f["stem"]),
                },
            }
            for f in self.source_files
        ]
        self._logger.info("1/3 Fetching data - Checking Date Coherence")
        self.genuine_source_date = len(set([f["date"] for f in self.source_files])) <= 1
        self.date = self.source_files[0]["date"]

    def fetch(self, conn: AzureBlobConnector):
        self._logger.info("1/3 Fetching data")
        self._last_version(conn)
        if self.genuine_source_date:
            self._logger.info("1/3 Fetching data - Downloading Source files")
            self._fetch(conn)
        else:
            self._logger.info(
                "1/3 Fetching data - WARNING - No Date Coherence between source files"
            )
        return self

    def process(self):
        self._logger.info("2/3 Process data")
        if self.genuine_source_date:
            self._process()
        else:
            self._logger.info(
                "2/3 Process data - WARNING - No Date Coherence between source files"
            )
        return self

    def write(self, conn: AzureBlobConnector):
        self._write(conn)
        return self

    def _fetch(self, conn: AzureBlobConnector):
        self.source_files = [
            {**f, **{"df": conn.open_as_dataframe(f["file"], sep=";")}}
            for f in self.source_files
        ]

    def _process(self):
        raise NotImplementedError

    def _write(self, conn: AzureBlobConnector):
        if self.genuine_source_date:
            self._logger.info("3/3 Writing data - Sending Files")
            conn.send_data(self.export_data, self.export_name)
        else:
            self._logger.info(
                "3/3 Writing data - WARNING - No Date Coherence between source files"
            )
