from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import EL_DEBER_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = EL_DEBER_URL
economy_section_url = EL_DEBER_URL + "/economia"


def el_deber_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the El Deber economy section and stores them in MongoDB.

    Args:
        timestamp_limit (datetime): The earliest date to scrape articles for. Articles older than this will stop the scraper.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: Returns 0 when the scraper stops due to reaching the timestamp limit.

    Notes:
        - Iterates through paginated article listings.
        - Checks for existing articles in the database to avoid duplicates.
        - Scrapes article content and saves new articles to the "USD_BOB_Parallel" collection.
    """
    current_page = 1
    while True:
        articles_page = economy_section_url + f"/{current_page}"
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
                                                     "source": "el_deber",
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
                                           "source": "el_deber",
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
    Scrapes a page of articles from the given URL in the El Deber economy section.

    Args:
        url (str): The URL of the article listing page to scrape.

    Returns:
        list: A list of dictionaries, each containing information about an article:
            - title (str): The article's title.
            - url (str): The full URL to the article.
            - date (datetime): The publication date of the article.
            - teaser (str or None): The teaser/summary of the article, if available.

    Notes:
        - Handles two possible date formats.
        - If the request fails, prints an error message and returns None.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find('div', class_='view-content').find_all('div', class_="mt-3", recursive=False)
        articles_list = []
        for article in articles_container:
            try:
                article_data = article.find('div').find_all('div', recursive=False)[1].find('div').find_all('div',
                                                                                                            recursive=False)
            except IndexError:
                article_data = article.find('div').find('div').find('div').find_all('div', recursive=False)
            article_date = article_data[1].find('div').text.strip()
            try:
                article_date = datetime.strptime(article_date, "%Y-%m-%d %H:%M")
            except ValueError:
                article_date = datetime.strptime(article_date, "%d/%m/%Y - %H:%M")
            article_url = base_url + article_data[2].find('a')['href']
            article_title = article_data[2].find('a').find('h2').text.strip()
            try:
                article_teaser = article_data[3].find('p').text
            except AttributeError:
                article_teaser = None
            article_dict = {
                "title": article_title,
                "url": article_url,
                "date": article_date,
                "teaser": article_teaser
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
        str: The concatenated text content of the article, with paragraphs separated by newlines.
             Returns an empty string if the request fails or no content is found.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        article = soup.find('div', class_='cuerpo-full').find('div').find_all('p', recursive=False)
        for paragraph in article:
            if paragraph.find('div'):
                continue
            elif paragraph.text.strip() == "":
                continue
            else:
                article_text += paragraph.text + "\n"
        return article_text
