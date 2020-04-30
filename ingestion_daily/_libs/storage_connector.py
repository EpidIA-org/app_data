import pandas as pd
from datetime import datetime


class StorageConnector:
    """Meta Storage Connector used to retrieve and send file in cloud storages
    Cloud Provider Agnostic
    # PART OF WBUTILS

    Raises:
        NotImplementedError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]

    Returns:
        [type]: [description]
    """

    CLOUD_PROVIDER = None

    def __init__(self, credentials, storage_name, **args):
        """Instantiate the connector

        Args:
            credentials ([type]): Needed credentials
            storage_name ([type]): Container/Bucket name
        """
        self.storage_name = storage_name
        self._conn = None

    def update_storage(self, storage_name: str):
        """Update the container/bucket name

        Args:
            storage_name (str): Container/Bucket name
        """
        self.storage_name = storage_name

    def is_file(self, filepath: str) -> bool:
        """Check whether or not the `filepath` is a genuine file in storage

        Args:
            filepath (str): File name in storage

        Raises:
            NotImplementedError: [description]

        Returns:
            bool: [description]
        """
        raise NotImplementedError

    def list_files(self, prefix: str = "") -> list:
        """List all the files in storage with a prefix

        Args:
            prefix (str, optional): Prefix to use to limit listed files. Defaults to "".

        Raises:
            NotImplementedError: [description]

        Returns:
            list: List of file names as strings
        """
        raise NotImplementedError

    def fetch_file(self, filepath: str) -> bytes:
        """Retrieve file from storage as bytes

        Args:
            filepath (str): File name to retrieve

        Raises:
            NotImplementedError: [description]

        Returns:
            bytes: Downloaded file as bytes
        """
        raise NotImplementedError

    def open_as_dataframe(self, filepath: str, **args) -> pd.DataFrame:
        """Retrieve file from storage as pandas DataFrame

        Args:
            filepath (str): File name to retrieve

        Raises:
            NotImplementedError: [description]

        Returns:
            pd.DataFrame: Downloaded file as dataframe
        """
        raise NotImplementedError

    def send_file(self, filepath: str, cloudpath):
        """Upload local file to storage

        Args:
            filepath (str): File to upload
            cloudpath ([type]): Storage location

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    def send_data(self, buff: str, cloudpath):
        """Upload buffer to storage

        Args:
            buff (str): String buffer
            cloudpath ([type]): Storage location

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    def get_last_filename_version(self, fileformat: str) -> str:
        """Returns max operator of `list_files`methods

        Note:
            This method is mostly used to retrieve the last version of a file.
            To do so, file names in storage should follow this naming convention:
            `<GENERIC_FILE_NAME>_<YYYY>-<MM>-<DD>.*`

        Args:
            fileformat (str): Generic file name to look for

        Returns:
            str: Most recent file in storage following the generic name
        """
        file_list = self.list_files(prefix=fileformat)
        return max(file_list)
