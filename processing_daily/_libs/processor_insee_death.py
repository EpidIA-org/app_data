import re
import pandas as pd
from zipfile import ZipFile
from io import BytesIO, StringIO
from _libs.processor import Processor
from _libs.azure_blob_connector import AzureBlobConnector


class WeeklyInseeDeathFigureProcessor(Processor):
    REQUIRED_FILE = [
        {
            "id": "insee_death",
            "stem": "{yyyy}-{mm}-{dd}_deces_quotidiens_departement_csv.zip",
        },
    ]
    DESTINATION_FILE = "formated-deces-quotidiens-departements-"
    _PROCESSOR_NAME = "WeeklyInseeDeathFigureProcessor"
    REGEX = "\d{4}-\d{2}-\d{2}_deces_quotidiens_departement_csv.zip"

    def __init__(self):
        super(WeeklyInseeDeathFigureProcessor, self).__init__()
        self.available_df = False

    def _last_version(self, conn: AzureBlobConnector):
        self._logger.info("1/3 Fetching data - Checking Source File Date")

        files = conn.list_files()
        file_name = max(
            [
                f
                for f in files
                if re.match("\d{4}-\d{2}-\d{2}_deces_quotidiens_departement_csv.zip", f)
            ]
        )

        self.source_files = [
            {
                **f,
                **{
                    "date": file_name[:10],
                    "file": conn.get_last_filename_version(file_name),
                },
            }
            for f in self.source_files
        ]
        self._logger.info("1/3 Fetching data - Checking Date Coherence")
        self.genuine_source_date = len(set([f["date"] for f in self.source_files])) <= 1
        self.date = file_name[:10]

    def _fetch(self, conn: AzureBlobConnector):
        self.source_files = [
            {**f, **{"bytes": BytesIO(conn.fetch_file(f["file"]))}}
            for f in self.source_files
        ]
        self.source_files = [
            {**f, **{"zip": ZipFile(f["bytes"])}} for f in self.source_files
        ]
        self.source_files = [
            {
                **f,
                **{
                    "data": StringIO(
                        f["zip"].open(f["zip"].namelist()[0]).read().decode("utf-8")
                    )
                },
            }
            for f in self.source_files
        ]
        self.source_files = [
            {**f, **{"df": pd.read_csv(f["data"], sep=";")}} for f in self.source_files
        ]

    def _process(self):
        self._assign_df()
        if self.available_df:
            self._processing()
        else:
            pass

    def _assign_df(self):
        self._logger.info("2/3 Process data - Assign Dataframes")
        df_insee = [f for f in self.source_files if f["id"] == "insee_death"]

        if len(df_insee):
            self.df_insee = df_insee[0]["df"]
            self.available_df = True

    def _processing(self):
        self._logger.info("2/3 Process data - Pandas Operations")
        df = self.df_insee

        # Date parsing and "Zone" attribute cleaning
        df["date"] = pd.to_datetime(df["Date_evenement"], format="%d/%m/%Y")
        df.drop(columns=["Date_evenement"], inplace=True)
        df.replace(to_replace=r"Dept_", value="", regex=True, inplace=True)
        df.replace(to_replace=r"France", value="00", regex=True, inplace=True)

        # Groups per "Zone"
        grouped = df.groupby(["Zone"])

        # Compute differences with a shift of 1 to get daily new figures, groups are used to avoid diff calculation at every first line of each "Zone"
        df["death_2018_day"] = grouped["Total_deces_2018"].diff()
        df["death_2019_day"] = grouped["Total_deces_2019"].diff()
        df["death_2020_day"] = grouped["Total_deces_2020"].diff()

        df["death_2018_demat_day"] = grouped[
            "Communes_a_envoi_dematerialise_Deces2018"
        ].diff()
        df["death_2019_demat_day"] = grouped[
            "Communes_a_envoi_dematerialise_Deces2019"
        ].diff()
        df["death_2020_demat_day"] = grouped[
            "Communes_a_envoi_dematerialise_Deces2020"
        ].diff()

        column_mapper = {
            "Zone": "dep",
            "date": "jour",
            "Communes_a_envoi_dematerialise_Deces2020": "death_2020_demat",
            "Total_deces_2020": "death_2020_total",
            "Communes_a_envoi_dematerialise_Deces2019": "death_2019_demat",
            "Total_deces_2019": "death_2019_total",
            "Communes_a_envoi_dematerialise_Deces2018": "death_2018_demat",
            "Total_deces_2018": "death_2018_total",
        }

        export_df = df.rename(columns=column_mapper).set_index(["dep", "jour"])
        self.export_data = export_df.to_csv(sep=";")
        self.export_name = f"{self.DESTINATION_FILE}{self.date}.csv"
