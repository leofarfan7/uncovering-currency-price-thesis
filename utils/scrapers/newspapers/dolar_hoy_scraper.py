from datetime import datetime

import requests
from bs4 import BeautifulSoup

from utils.mongo_controller import mongo_controller


def dolar_hoy_scraper(url):
    """
    Scrapes the blue dollar price from the given URL and saves the record to the database.

    Args:
        url (str): The URL to scrape the blue dollar price from.

    Returns:
        None
    """
    # Get today's date, with hour and minute set to 00:00
    timestamp = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML response
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            # Extract the price from the HTML
            price = soup.find('div', class_='venta').find('div', class_='valor').text
            price = float(price[1:])  # Convert the price to a float
        except AttributeError:
            # Handle the case where the price could not be found in the HTML
            print(f"[dolar_hoy_scraper] Error: Could not find the price in the HTML response.")
            price = None
    else:
        # Handle the case where the request was not successful
        print(f"[dolar_hoy_scraper] Code {response.status_code}: Could not connect to the URL.")
        price = None

    # Save the blue dollar price record to the database
    existing_record = mongo_controller.query_data(_mode="one",
                                                  collection="USD_ARS_Parallel",
                                                  _filter={
                                                      "timestamp": timestamp,
                                                      "metadata.fiat": "ARS",
                                                      "metadata.source": "dolar_hoy"
                                                  })
    if not existing_record:
        mongo_controller.save_data(collection="USD_ARS_Parallel",
                                   data={
                                       "timestamp": timestamp,
                                       "metadata": {
                                           "fiat": "ARS",
                                           "source": "dolar_hoy"
                                       },
                                       "sell_price": price
                                   })

    print(f"[dolar_hoy_scraper] Price retrieved successfully.")
    return 0
