import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import EL_DIARIO_URL, USER_AGENT_HEADERS
from utils.mongo_controller import mongo_controller

base_url = EL_DIARIO_URL
economy_section_url = EL_DIARIO_URL + "/portal/category/secciones/economia"


def el_diario_scraper(timestamp_limit, debug):
    current_page = 1
    while True:
        articles_page = economy_section_url + f"/page/{current_page}"
        if debug:
            print("--------------------")
            print(f"Articles Page: {articles_page}")
        articles = article_page_scraper(articles_page, current_page)
        for article in articles:
            if debug:
                print("---")
                print(f"Article: {article}")
            exists = mongo_controller.query_data(_mode="one",
                                                 collection="USD_BOB_Parallel",
                                                 _filter={
                                                     "timestamp": article["date"],
                                                     "source": "el_diario",
                                                     "title": article["title"]
                                                 })
            if debug:
                print(f"Exists: {exists}")
            if article["date"] < timestamp_limit:
                return 0
            if exists is not None:
                continue
            article["teaser"], article["content"] = article_scraper(article["url"])
            if debug:
                print(f"Complete Article: {article}")
            mongo_controller.save_data(collection="USD_BOB_Parallel",
                                       data={
                                           "timestamp": article["date"],
                                           "source": "el_diario",
                                           "title": article["title"],
                                           "teaser": article["teaser"],
                                           "url": article["url"],
                                           "content": article["content"],
                                           "first_stage_processed": False,
                                           "second_stage_processed": None
                                       })
        current_page += 1


def article_page_scraper(url, page):
    """
    Scrapes article metadata from a given El Diario economy section page.

    Args:
        url (str): The URL of the page to scrape.
        page (int): The page number being scraped (used to determine section parsing).

    Returns:
        list: A list of dictionaries, each containing:
            - title (str): The article's title.
            - url (str): The article's URL.
            - date (datetime): The publication date of the article.

    Notes:
        - For the first page, both highlighted (newest) and older articles are parsed.
        - For subsequent pages, only older articles are parsed.
        - If the request fails, prints an error message and returns None.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html5lib')
        articles_to_examine = []
        articles_list = []
        date_pattern = r"https://www\.eldiario\.net/portal/(\d{4}/\d{2}/\d{2})/"
        general_articles_container = soup.select_one('div.tdc-zone#tdi_44').find('div').find_all('div', recursive=False)

        if page == 1:
            # Newest articles section (highlighted)
            newest_articles_container = general_articles_container[0].find('div', class_='wpb_wrapper')
            newest_articles_container = newest_articles_container.find_all('div', recursive=False)[2].find('div')
            newest_articles_container = newest_articles_container.find_all('div', recursive=False)
            # NS Article 1
            first_article_info = newest_articles_container[0].find('div', class_='td-module-meta-info').find('div')
            articles_to_examine.append(first_article_info)
            # NS Articles 2-5
            next_articles = newest_articles_container[1].find_all('div', recursive=False)
            for article in next_articles:
                articles_to_examine.append(article.find('div', class_='td-module-meta-info').find('div'))

        # Older articles section
        older_articles_container = general_articles_container[1].find('div', class_='wpb_wrapper')
        older_articles_container = older_articles_container.find('div', class_='tdi_58').find('div', id='tdi_58')
        older_articles_container = older_articles_container.find_all('div', recursive=False)
        for article in older_articles_container:
            articles_to_examine.append(article.find('div').find('div', class_='td-module-meta-info').find('h3'))

        # For every article
        for article in articles_to_examine:
            title = article.find('a').text.replace("  ", " ")
            url = article.find('a')['href']
            date = re.search(date_pattern, url).group(1)
            date = datetime.strptime(date, "%Y/%m/%d")
            article_dict = {
                "title": title,
                "url": url,
                "date": date
            }
            articles_list.append(article_dict)
        return articles_list
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def article_scraper(url):
    """
    Scrapes the teaser and full content of an article from the given URL.

    Args:
        url (str): The URL of the article to scrape.

    Returns:
        tuple: A tuple containing:
            - article_teaser (str or None): The teaser text of the article, or None if not found.
            - article_text (str): The full text content of the article.
    """
    # Send an HTTP GET request to the URL
    response = requests.get(url, headers=USER_AGENT_HEADERS)

    # Check if the request was successful
    if response.status_code == 200:
        article_text = ""
        soup = BeautifulSoup(response.text, 'html5lib')
        article_container = soup.find('div', id='tdi_51').find('div', class_='tdi_54')
        article_container = article_container.find('div', class_='wpb_wrapper')
        try:
            article_teaser = article_container.find('div', class_='tdb_single_subtitle').find('p').text.strip(
                ">").strip()
        except AttributeError:
            article_teaser = None
        article_body = article_container.find('div', class_='tdb_single_content').find('div').find_all('p',
                                                                                                       recursive=False)
        for paragraph in article_body:
            article_text += paragraph.text + "\n"
        article_text = article_text.strip()
        return article_teaser, article_text
