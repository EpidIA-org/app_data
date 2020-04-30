from .processor import Processor

class DailyCovidFiguresProcessor(Processor):
    REQUIRED_FILE = [
        {"id": "full_data", "stem": "donnees-hospitalieres-covid19-"},
        {"id": "new_data", "stem": "donnees-hospitalieres-nouveaux-covid19-"},
    ]
    DESTINATION_FILE = "formated-covid-data-from-datagouvfr-"
    _PROCESSOR_NAME = "DailyCovidFiguresProcessor"

    COLUMNS_MAPPER = {
        "hosp": "current_hosp",
        "rea": "current_critical",
        "rad": "total_returned_home",
        "dc": "total_death",
        "incid_hosp": "new_hosp",
        "incid_rea": "new_critical",
        "incid_rad": "new_returned_home",
        "incid_dc": "new_death",
        "cumsum_incid_hosp": "cumulative_hosp",
        "cumsum_incid_rea": "cumulative_critical",
        "cumsum_incid_rad": "cumulative_returned_home",
        "cumsum_incid_dc": "cumulative_death",
        "men_hosp": "current_men_hosp",
        "men_rea": "current_men_critical",
        "men_rad": "total_men_returned_home",
        "men_dc": "total_men_death",
        "women_hosp": "current_women_hosp",
        "women_rea": "current_women_critical",
        "women_rad": "total_women_returned_home",
        "women_dc": "total_women_death",
    }

    def __init__(self):
        super(DailyCovidFiguresProcessor, self).__init__()
        self.available_df = False

    def _process(self):
        self._assign_df()
        if self.available_df:
            self._processing()
        else:
            pass

    def _assign_df(self):
        self._logger.info("2/3 Process data - Assign Dataframes")
        df_by_total = [f for f in self.source_files if f["id"] == "full_data"]
        df_by_new = [f for f in self.source_files if f["id"] == "new_data"]

        if len(df_by_total) and len(df_by_new):
            self.df_by_total = df_by_total[0]["df"]
            self.df_by_new = df_by_new[0]["df"]
            self.available_df = True

    def _processing(self):
        self._logger.info("2/3 Process data - Pandas Operations")
        global_df = (
            self.df_by_total.query("sexe == 0")
            .set_index(["dep", "jour"])
            .filter(items=["hosp", "rea", "rad", "dc"])
            .reset_index()
            .groupby(["dep", "jour"])
            .sum()
        )
        gender_df = (
            self.df_by_total.query("sexe != 0")
            .set_index(["dep", "jour"])
            .filter(items=["hosp", "rea", "rad", "dc"])
            .reset_index()
            .groupby(["dep", "jour"])
            .sum()
            .add_prefix("gen_")
        )
        cumsum_new_df = (
            self.df_by_new.groupby(["dep", "jour"])
            .sum()
            .groupby(level=0)
            .apply(lambda x: x.cumsum())
            .add_prefix("cumsum_")
            .join(self.df_by_new.groupby(["dep", "jour"]).sum())
        )
        men_df = (
            self.df_by_total.query("sexe == 1")
            .drop(columns=["sexe"])
            .groupby(["dep", "jour"])
            .sum()
            .add_prefix("men_")
        )
        women_df = (
            self.df_by_total.query("sexe == 2")
            .drop(columns=["sexe"])
            .groupby(["dep", "jour"])
            .sum()
            .add_prefix("women_")
        )
        full_df = (
            global_df.join(cumsum_new_df, how="inner")
            .join(men_df, how="inner")
            .join(women_df, how="inner")
        )
        self.export_data = full_df.rename(columns=self.COLUMNS_MAPPER).to_csv(sep=';')
        self.export_name = f"{self.DESTINATION_FILE}{self.date}.csv"
