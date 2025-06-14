from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import ECONOMY_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = ECONOMY_URL
economy_section_url = ECONOMY_URL + "/blog/section/economia"


def economy_scraper(timestamp_limit, debug):
    """
    Scrapes articles from the economy section, saving new articles to the database until a timestamp limit is reached.

    Args:
        timestamp_limit (datetime): The earliest article date to scrape. Scraping stops when an article older than this is found.
        debug (bool): If True, prints debug information during scraping.

    Returns:
        int: 0 when the timestamp limit is reached.
    """
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
                                                     "source": "economy",
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
                                           "source": "economy",
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
    Fetches and parses a page of economy articles, extracting metadata for each article.

    Args:
        url (str): The URL of the economy section page to scrape.

    Returns:
        list: A list of dictionaries, each containing the title, URL, date, and teaser of an article.
              Returns an empty list if the request fails.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_container = soup.find('div', class_='archive-contents').find('div', class_='row').find_all('div',
                                                                                                            class_='archive-item')
        articles_list = []
        for article in articles_container:
            article = article.find('article').find('div', class_='article-data')
            title = article.find('h2', class_='title').find('a').text.strip()
            url = article.find('h2', class_='title').find('a')['href']
            url = base_url + url
            date = article.find('div', class_="content-info").find('span', class_='date-container').text.strip()
            date = datetime.strptime(date, "%d/%m/%y")
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
    Fetches and extracts the main text content from an article page.

    Args:
        url (str): The URL of the article to scrape.

    Returns:
        str: The extracted article text, or an empty string if not found.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        try:
            # Try to find the main article content
            article_body = soup.find('div', class_='content-data').find('div', class_='body').find_all('p')
        except AttributeError:
            # Fallback for articles with video content
            article_body = soup.find('div', class_='video-data').find('div', class_='body').find_all('p')
        for paragraph in article_body:
            article_text += paragraph.text.strip() + "\n"
        article_text.strip()
        return article_text
