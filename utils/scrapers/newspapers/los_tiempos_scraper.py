from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import LOS_TIEMPOS_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = LOS_TIEMPOS_URL
economy_section_url = LOS_TIEMPOS_URL + "/hemeroteca/seccion/actualidad-1/seccion/economia-26149?contenido=&sort_by=field_noticia_fecha"


def los_tiempos_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the Los Tiempos economy section and stores them in MongoDB.

    Args:
        timestamp_limit (datetime): The earliest date to scrape articles for. Articles older than this will stop the scraper.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when the scraper stops due to reaching the timestamp limit.
    """
    current_page = 0
    while True:
        articles_page = economy_section_url + f"&page={current_page}"
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
                                                     "source": "los_tiempos",
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
                                           "source": "los_tiempos",
                                           "title": article["title"],
                                           "teaser": article["teaser"],
                                           "url": article["url"],
                                           "content": article["content"].strip(),
                                           "first_stage_processed": False,
                                           "second_stage_processed": None
                                       })
        current_page += 1


def article_page_scraper(url):
    """
    Scrapes a page of article listings from the given URL in the Los Tiempos economy section.

    Args:
        url (str): The URL of the article listing page to scrape.

    Returns:
        list: A list of dictionaries, each containing information about an article:
            - title (str): The article's title.
            - url (str): The full URL to the article.
            - date (datetime): The publication date of the article.
            - teaser (None): Placeholder for teaser text (currently always None).

    Prints an error message if the request fails.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find('div', id='content').find('div', class_='region-three-25-50-25-second')
        articles_container = articles_container.find('div').find_all('div', recursive=False)[2]
        articles_container = articles_container.find('div', class_='view-content').find_all('div', class_='views-row')
        articles_list = []
        for article in articles_container:
            title = article.find('div', class_='views-field-title').find('a').text
            url = article.find('div', class_='views-field-title').find('a')['href']
            url = base_url + url
            date = article.find('span', class_='date-display-single').text
            date = datetime.strptime(date, "%d/%m/%Y")
            article_dict = {
                "title": title,
                "url": url,
                "date": date,
                "teaser": None
            }
            articles_list.append(article_dict)
        return articles_list
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def article_scraper(url):
    """
    Scrapes the main content of a news article from the given URL.

    Args:
        url (str): The URL of the article to scrape.

    Returns:
        str: The concatenated text content of all paragraphs in the article body,
             separated by newline characters. Returns an empty string if the request fails.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        article_body = soup.find('div', class_='node-content').find('div', class_='body')
        article_body = article_body.find('div', class_='field-item').find_all('p', recursive=False)
        for paragraph in article_body:
            article_text += paragraph.text + "\n"
        return article_text
