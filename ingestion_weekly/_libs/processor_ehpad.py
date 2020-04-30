from _libs.processor import Processor


class DailyEhpadFiguresProcessor(Processor):
    REQUIRED_FILE = [
        {"id": "ehpad_data", "stem": "covid-19-with-ephad_"},
    ]
    DESTINATION_FILE = "formated-covid-data-width-ehpad-"
    _PROCESSOR_NAME = "DailyEhpadFiguresProcessor"

    def __init__(self):
        super(DailyEhpadFiguresProcessor, self).__init__()
        self.available_df = False

    def _process(self):
        self._assign_df()
        if self.available_df:
            self._processing()
        else:
            pass

    def _assign_df(self):
        self._logger.info("2/3 Process data - Assign Dataframes")
        df_by_ephad = [f for f in self.source_files if f["id"] == "ehpad_data"]

        if len(df_by_ephad):
            self.df_by_ephad = df_by_ephad[0]["df"]
            self.available_df = True

    def _processing(self):
        self._logger.info("2/3 Process data - Pandas Operations")
        self.df_by_ephad = self.df_by_ephad.set_index(["dep", "jour", "sexe"])[
            ["dc", "dc ehpad", "dc quot", "dc ehpad quot"]
        ].reset_index()
        all_df = (
            self.df_by_ephad.reset_index()
            .query("sexe == 0")
            .drop(columns=["sexe"])
            .rename(
                columns={
                    "dc": "total_deaths",
                    "dc ehpad": "total_deaths_ehpad",
                    "dc quot": "new_deaths",
                    "dc ehpad quot": "new_deaths_ehpad",
                }
            )
            .groupby(["dep", "jour"])
            .sum()
        )
        men_df = (
            self.df_by_ephad.reset_index()
            .query("sexe == 1")
            .drop(columns=["sexe"])
            .rename(
                columns={
                    "dc": "total_deaths",
                    "dc ehpad": "total_deaths_ehpad",
                    "dc quot": "new_deaths",
                    "dc ehpad quot": "new_deaths_ehpad",
                }
            )
            .groupby(["dep", "jour"])
            .sum()
            .add_prefix("men_")
        )
        women_df = (
            self.df_by_ephad.reset_index()
            .query("sexe == 2")
            .drop(columns=["sexe"])
            .rename(
                columns={
                    "dc": "total_deaths",
                    "dc ehpad": "total_deaths_ehpad",
                    "dc quot": "new_deaths",
                    "dc ehpad quot": "new_deaths_ehpad",
                }
            )
            .groupby(["dep", "jour"])
            .sum()
            .add_prefix("women_")
        )
        full_df = all_df.join(men_df, how="inner").join(women_df, how="inner")
        self.export_data = full_df.to_csv(sep=";")
        self.export_name = f"{self.DESTINATION_FILE}{self.date}.csv"
