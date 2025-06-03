import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import BRUJULA_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = BRUJULA_URL
economy_section_url = base_url + "/economia"


def brujula_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the 'Brújula Digital' economy section, saving new articles to MongoDB.

    Args:
        timestamp_limit (datetime): The earliest date for articles to scrape. Stops when an article older than this is found.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when the scraping process is stopped due to reaching the timestamp limit.
    """
    current_page = 1
    while True:
        articles_page = economy_section_url + f"/p={current_page}"
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
                                                     "source": "brujula",
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
                                           "source": "brujula",
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
    Scrapes a page of articles from the given URL in the 'Brújula Digital' economy section.

    Args:
        url (str): The URL of the articles page to scrape.

    Returns:
        list: A list of dictionaries, each containing information about an article:
            - title (str): The article's title.
            - url (str): The article's URL.
            - date (datetime or str): The publication date as a datetime object if found, otherwise the URL.
            - teaser (None): Placeholder for teaser text (currently always None).

    Prints an error message if the page cannot be retrieved.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_containers = soup.find_all('ul', class_='otras-not')
        articles_list = []
        for container in articles_containers:
            articles = container.find_all('li')
            for article in articles:
                article = article.find_all('a')[1]
                title = article.text.strip()
                url = article['href']
                date = url
                pattern = r"/(\d{4})/(\d{2})/(\d{2})/"
                match = re.search(pattern, date)
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    day = int(match.group(3))

                    # Create a datetime object
                    date = datetime(year, month, day)
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
    Scrapes the main content of an article from the given URL.

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
        article_body = soup.find('div', class_='contIn').find_all('p')
        for paragraph in article_body:
            article_text += paragraph.text.strip() + "\n"
        article_text.strip()
        return article_text
