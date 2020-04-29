from urllib.request import urlopen
from _libs.scrapper import Scrapper


class DataGouvScrapper(Scrapper):
    _SCRAPPER_NAME = "DataGouvScrapper"
    URL = "https://www.data.gouv.fr/en/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/"

    def __init__(self):
        super(DataGouvScrapper, self).__init__()

    def _fetch(self):
        self._fetch_datagouv_files()
        self._fetch_ehpad_file()

    def _fetch_ehpad_file(self):
        self._logger.info(
            "1/2 Fetching data - EHPAD - Get resources-community from page"
        )
        soup_articles = self._soup.findAll(
            "article", {"class": "card resource-card resource-card-community"}
        )
        for soup_article in soup_articles:
            try:
                article_name = soup_article.find("h4", {"class": "ellipsis"}).contents[
                    0
                ]
                if "covid-19.csv" in article_name:
                    soup_ephad = soup_article
                    break
            except Exception as e:
                self._logger.info(e)

        # Generic file name covid-19.csv DD/MM/YYYY ->
        self._logger.info("1/2 Fetching data - EHPAD - Generating Name")
        file_name = article_name.split(" ")[1].split("/")
        file_name = (
            f"covid-19-with-ephad_{file_name[2]}-{file_name[1]}-{file_name[0]}.csv"
        )
        href_ephad = soup_ephad.find("a", href=True)["href"]
        self.data_to_retrieve.append(
            {"name": file_name, "data": urlopen(href_ephad).read()}
        )

    def _fetch_datagouv_files(self):
        # Get the div.resources-list
        self._logger.info("1/2 Fetching data - DATAGOUV - Get resources-list from page")
        resources_list = self._soup.findAll("div", {"class": "resources-list"})[0]

        # In div.resources-list get every article
        self._logger.info(
            "1/2 Fetching data - DATAGOUV - Get articles from resources-list"
        )
        articles = resources_list.findAll("article")

        # for each article extract the h4 as the file name
        self._logger.info("1/2 Fetching data - DATAGOUV - Get file names from articles")
        file_names = [article.find("h4").text for article in articles]

        # for each article get the download btnfor article in articles]
        self._logger.info(
            "1/2 Fetching data - DATAGOUV - Get download link from articles"
        )
        divs_of_interest = [
            article.footer.find("div", {"class": "resource-card-actions btn-toolbar"})
            for article in articles
        ]
        a_href_list = [
            div.findAll("a")[1]
            for div in divs_of_interest
            if len(div.findAll("a")) >= 2
        ]
        links = [a.get("href", None) for a in a_href_list]

        # Download Files
        self._logger.info("1/2 Fetching data - DATAGOUV - Download Files")
        self.data_to_retrieve += [
            {"name": file_names[i], "data": urlopen(href).read()}
            for i, href in enumerate(links)
            if href is not None
        ]
