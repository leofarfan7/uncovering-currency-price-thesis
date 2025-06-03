from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import OXIGENO_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = OXIGENO_URL
economy_section_url = base_url + "/politica"


def oxigeno_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the Oxigeno newspaper's 'politica' section, saving new articles to MongoDB.

    Args:
        timestamp_limit (datetime): The earliest date for articles to scrape. Stops when an article older than this is found.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when scraping is stopped due to reaching the timestamp limit.
    """
    current_page = 0
    while True:
        articles_page = economy_section_url + f"?page={current_page}"
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
                                                     "source": "oxigeno",
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
                                           "source": "oxigeno",
                                           "title": article["title"],
                                           "teaser": article["teaser"],
                                           "url": article["url"],
                                           "content": article["content"],
                                           "first_stage_processed": False,
                                           "second_stage_processed": None
                                       })
        current_page += 1


def article_page_scraper(url):
    """
    Scrapes a page of articles from the given URL in the Oxigeno newspaper's 'politica' section.

    Args:
        url (str): The URL of the articles page to scrape.

    Returns:
        list: A list of dictionaries, each containing information about an article:
            - title (str): The article's title.
            - url (str): The full URL to the article.
            - date (datetime): The publication date and time of the article.
            - teaser (str): A short teaser or summary of the article.

    Prints an error message if the page cannot be retrieved.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find_all('div', class_='node-noticia')
        articles_list = []
        for article in articles_container:
            title = article.find('div', class_='field-name-title').find('a').text.strip()
            url = article.find('div', class_='field-name-title').find('a')['href']
            url = base_url + url
            date = article.find('div', class_='field-name-published-on').find('div').find('div').text.strip()
            date = datetime.strptime(date, "%d/%m/%Y - %H:%M")
            teaser = article.find('div', class_='field-name-body').text.strip()
            article_dict = {
                "title": title,
                "url": url,
                "date": date,
                "teaser": teaser
            }
            articles_list.append(article_dict)
        return articles_list
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def article_scraper(url):
    """
    Scrapes the full content of an article from the given URL.

    Args:
        url (str): The URL of the article to scrape.

    Returns:
        str: The full text content of the article, with paragraphs separated by newlines.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        article_body = soup.find('div', class_='field-name-body').find_all('p')
        for paragraph in article_body:
            article_text += paragraph.text.strip() + "\n"
        article_text.strip()
        return article_text
