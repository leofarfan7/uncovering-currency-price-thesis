import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import ProtocolError

from config import RED_UNO_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = RED_UNO_URL
economy_section_url = RED_UNO_URL + "/j/economia"


def red_uno_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the Red Uno economy section, saving new articles to MongoDB.

    Args:
        timestamp_limit (datetime): The earliest date for articles to scrape. Articles older than this are ignored.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when an article older than timestamp_limit is found, ending the scraping process.
    """
    current_page = 0
    while True:
        articles_page = economy_section_url + f"/{current_page}"
        if debug:
            print("--------------------")
            print(f"Articles Page: {articles_page}")
        try:
            articles = article_page_scraper(articles_page)
        except requests.exceptions.ChunkedEncodingError or ProtocolError:
            print("Error in server response. Retrying...")
            time.sleep(20)
            continue
        for article in articles:
            if debug:
                print("---")
                print(f"Article: {article}")
            exists = mongo_controller.query_data(_mode="one",
                                                 collection="USD_BOB_Parallel",
                                                 _filter={
                                                     "source": "red_uno",
                                                     "title": article["title"]
                                                 })
            if debug:
                print(f"Exists: {exists}")
            try:
                article["teaser"], article["date"], article["content"] = article_scraper(article["url"])
            except requests.exceptions.ChunkedEncodingError or ProtocolError:
                print("Error in server response. Retrying...")
                time.sleep(20)
                continue
            if article["date"] < timestamp_limit:
                return 0
            if exists is not None:
                continue
            if debug:
                print(f"Complete Article: {article}")
            mongo_controller.save_data(collection="USD_BOB_Parallel",
                                       data={
                                           "timestamp": article["date"],
                                           "source": "red_uno",
                                           "title": article["title"],
                                           "teaser": article["teaser"],
                                           "url": article["url"],
                                           "content": article["content"],
                                           "first_stage_processed": False,
                                           "second_stage_processed": None
                                       })
        current_page += 12


def article_page_scraper(url):
    """
    Scrapes a page of articles from the given URL in the Red Uno economy section.

    Args:
        url (str): The URL of the articles page to scrape.

    Returns:
        list: A list of dictionaries, each containing the title and URL of an article.

    Raises:
        ConnectionError: If the page cannot be retrieved after 5 attempts.
    """
    attempts = 0
    while True:
        # Send an HTTP GET request to the URL
        response = requests.get(url, headers=USER_AGENT_HEADERS)
        attempts += 1
        # Check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html5lib')
            articles_container = soup.find_all('div', class_='titulo')
            articles_list = []
            for article in articles_container:
                article_title = article.find('h2').text
                article_url = article.find('a')['href']
                article_url = base_url + article_url
                article_dict = {
                    "title": article_title,
                    "url": article_url
                }
                articles_list.append(article_dict)
            return articles_list
        elif attempts < 5:
            print(f"Status code: {response.status_code}. Retrying...")
            time.sleep(20)
        else:
            raise ConnectionError(f"Failed to retrieve the page. Status code: {response.status_code}")


def article_scraper(url):
    """
    Scrapes a single article from the given URL.

    Args:
        url (str): The URL of the article to scrape.

    Returns:
        tuple: A tuple containing:
            - article_teaser (str): The teaser or introduction of the article.
            - article_date (datetime): The publication date of the article as a datetime object.
            - article_text (str): The full text content of the article.

    Raises:
        ConnectionError: If the page cannot be retrieved after 5 attempts.
        ValueError: If the date format does not match the expected patterns.
    """
    attempts = 0
    while True:
        # Send an HTTP GET request to the URL
        response = requests.get(url, headers=USER_AGENT_HEADERS)
        attempts += 1
        # Check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html5lib')
            article_text = ""
            header = soup.find('div', class_='grid-encabezado')
            article_teaser = header.find('p', class_='intro').text
            article_date = header.find('p', class_='fecha').text.strip()
            try:
                article_date = datetime.strptime(article_date, "%d/%m/%Y %I:%M %p")
            except ValueError:
                article_date = datetime.strptime(article_date, "%d/%m/%Y %H:%M")
            article_body = soup.find('div', class_='body__cuerpo').find_all('p', recursive=False)
            for paragraph in article_body:
                article_text += paragraph.text + "\n"
            article_text.strip()
            return article_teaser, article_date, article_text
        elif attempts < 5:
            print(f"Status code: {response.status_code}. Retrying...")
            time.sleep(20)
        else:
            raise ConnectionError(f"Failed to retrieve the page. Status code: {response.status_code}")
