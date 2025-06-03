import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import FIDES_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = FIDES_URL
economy_section_url = FIDES_URL + "/economia"


def fides_scraper(timestamp_limit, debug):
    current_page = 1
    while True:
        articles_page = economy_section_url + f"/?page={current_page}"
        if debug:
            print("--------------------")
            print(f"Articles Page: {articles_page}")
        articles = article_page_scraper(articles_page)
        for article in articles:
            if debug:
                print("---")
                print(f"Article: {article}")
            exists = mongo_controller.query_data(_mode="one",
                                                 collection="USD_BOB_Parallel",
                                                 _filter={
                                                     "timestamp": article["date"],
                                                     "source": "fides",
                                                     "title": article["title"]
                                                 })
            if debug:
                print(f"Exists: {exists}")
            if article["date"] < timestamp_limit:
                return 0
            if exists is not None:
                continue
            article["content"] = article_scraper(article["url"])
            if debug:
                print(f"Complete Article: {article}")
            mongo_controller.save_data(collection="USD_BOB_Parallel",
                                       data={
                                           "timestamp": article["date"],
                                           "source": "fides",
                                           "title": article["title"],
                                           "teaser": article["teaser"],
                                           "url": article["url"],
                                           "content": article["content"],
                                           "first_stage_processed": False,
                                           "second_stage_processed": None
                                       })
        current_page += 1


def article_page_scraper(url):
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find_all('div', class_='nws-item')
        articles_list = []
        for article in articles_container:
            title = article.find('div', class_='qtitle').find('a').text.strip()
            url = article.find('div', class_='qtitle').find('a')['href']
            url = base_url + url
            date = article.find('div', class_='qdate').text.strip()
            pattern = r"(\d{1,2}) de (\w+), (\d{4}) - (\d{2}:\d{2})"
            match = re.search(pattern, date)
            if match:
                day = int(match.group(1))
                month_str = match.group(2)
                year = int(match.group(3))
                time_str = match.group(4)

                # Map Spanish month names to month numbers
                months = {
                    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
                    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
                    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
                }

                month = months[month_str.lower()]

                # Create a datetime object
                date_time_str = f"{year}-{month:02d}-{day:02d} {time_str}"
                date = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")
            article_dict = {
                "title": title,
                "url": url,
                "date": date,
                "teaser": None
            }
            articles_list.append(article_dict)
        return articles_list
    else:
        # TODO: See what to do with errors. Maybe write all of them to a file?
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def article_scraper(url):
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        article_body = soup.find('div', class_='qtexto').find_all('p')
        for paragraph in article_body:
            article_text += paragraph.text.strip() + "\n"
        article_text.strip()
        return article_text
