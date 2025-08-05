import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

class Scraper:
    def get_soup(self, url: str) -> BeautifulSoup | None:
        # validate ability to scrape this site
        if not self.is_allowed(url):
            print(f"Unable to scrape {url} due to robots.txt.")
            return None

        # get url html
        resp = requests.get(url)

        # validate retrieval and convert to soup 
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser')
        else:
            raise ScraperUrlError(f"Received status code: {resp.status_code}")
        
    def is_allowed(self, url: str, user_agent = "*"):
        # parse url 
        parsed_url = urlparse(url)
        # get base url
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        # robots.txt url
        robots_url = f"{base_url}/robots.txt"

        # read the robots.txt
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()

        # validate permission for scraping and return boolean 
        return rp.can_fetch(user_agent, url)


class AquariumWiki(Scraper):
    def __init__(self): 
        self._url = "https://en.wikipedia.org/wiki/List_of_aquaria_by_country"

    def get_content(self):
        try:
            soup = self.get_soup(self._url)
            info = {}

            if soup:
                main_content = soup.find("div", class_="mw-content-ltr mw-parser-output")
                for tag in main_content.find_all(["h3", "h2", "ul"]):  
                    print(tag)
                    if tag.name == "h2" and tag.get("id") == "See_also":
                        break
                    if tag.name == "h2":
                        continent = tag.text
                        info.setdefault(continent, {})
                    if tag.name == "h3":
                        country = tag.text
                        info.get(continent, {}).setdefault(country, {})
                    if tag.name == "ul":
                        for li_tag in tag.find_all("a"):
                            country_dict = info.get(continent).get(country)
                            country_dict.setdefault("link", li_tag.get("href"))
                            country_dict.setdefault("name", li_tag.text)
    
        except ScraperUrlError as e:
            print(e)
        
        

class ScraperUrlError(Exception):
    def __init__(self, msg: str):
        super().__init__(f"Invalid URL: {msg}")


if __name__ == "__main__":
    aw = AquariumWiki()
    info_dict = aw.get_content()



