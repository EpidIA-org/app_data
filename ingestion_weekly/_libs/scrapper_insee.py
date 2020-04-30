from .scrapper import Scrapper
from urllib.parse import urlsplit
from urllib.request import urlopen


class InseeScrapper(Scrapper):
    def __init__(self):
        super(InseeScrapper, self).__init__()

    _SCRAPPER_NAME = "InseeScrapper"
    URL = "https://www.insee.fr/fr/information/4470857"

    def _fetch(self):
        split_url = urlsplit(self.URL)
        url_base = split_url.scheme + "://" + split_url.hostname
        resources_list = self._soup.findAll("div", {"class": "bloc fichiers"})
        # Only the last element of each div (csv link) is to be downloaded
        content_list = [b.contents[-1] for b in resources_list]
        file_names = [
            content.attrs["href"].rsplit("/", 1)[-1] for content in content_list
        ]
        file_paths = [url_base + content.attrs["href"] for content in content_list]
        self.data_to_retrieve = [
            {"name": file_names[i], "data": urlopen(file_paths[i]).read()}
            for i, file_name in enumerate(file_names)
        ]
