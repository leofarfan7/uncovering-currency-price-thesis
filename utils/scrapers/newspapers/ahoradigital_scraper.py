from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import AHORADIGITAL_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = AHORADIGITAL_URL
economy_section_url = base_url + "/category/economia"


def ahoradigital_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the 'Economía' section of the Ahoradigital website, starting from the most recent,
    and saves new articles to the MongoDB collection 'USD_BOB_Parallel' until an article older than
    `timestamp_limit` is found.

    Args:
        timestamp_limit (datetime): The oldest article date to scrape. Stops scraping when an article older than this is found.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when the scraping process is stopped due to reaching the timestamp limit.
    """
    current_page = 1
    while True:
        articles_page = economy_section_url + f"/page/{current_page}"
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
                                                     "source": "ahoradigital",
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
                                           "source": "ahoradigital",
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
    Scrapes a page of articles from the given URL in the 'Economía' section of Ahoradigital.

    Args:
        url (str): The URL of the articles page to scrape.

    Returns:
        list: A list of dictionaries, each containing information about an article:
            - title (str): The article's title.
            - url (str): The URL to the full article.
            - date (datetime): The publication date of the article.
            - teaser (None): Placeholder for teaser text (currently always None).

    Prints an error message if the page cannot be retrieved.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find('div', class_='jeg_main_content').find_all('article', class_='jeg_post')
        articles_list = []
        for article in articles_container:
            title = article.find('h3', class_='jeg_post_title').find('a').text.strip()
            url = article.find('h3', class_='jeg_post_title').find('a')['href']
            date = article.find('div', class_='jeg_post_meta').find('a').text.strip()
            date = datetime.strptime(date, "%Y/%m/%d")
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
    Scrapes the full content of an article from the given URL on the Ahoradigital website.

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
        article_body = soup.find('div', class_='content-inner').find_all('p')
        for paragraph in article_body:
            article_text += paragraph.text.strip() + "\n"
        article_text.strip()
        return article_text
