import json

import requests

import config
from utils.data_processing import filter_ad

# Define the endpoint
url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"


def binance_request(timestamp, fiat="BOB", crypto="USDT", debug=False):
    """
    Scrape Binance P2P buy and sell pages and save the raw data to MongoDB.

    This function scrapes the Binance P2P buy and sell pages for the specified fiat and crypto currencies.
    It then saves the scraped raw data to MongoDB.

    Args:
        timestamp (datetime.now()): The timestamp of the request.
        fiat (str, optional): The fiat currency. Defaults to "BOB".
        crypto (str, optional): The cryptocurrency. Defaults to "USDT".
        debug (bool, optional): If True, saves the raw response to a file for debugging. Defaults to False.

    Returns:
        tuple: A tuple containing sell_data and buy_data lists.
    """
    print(f"\n[binanceRequest] Scraping Binance P2P buy page...")
    sell_data = ads_page_request(fiat=fiat, crypto=crypto, trade_type="BUY", timestamp=timestamp,
                                 debug=debug)
    print(f"\n[binanceRequest] Scraping Binance P2P sell page...")
    buy_data = ads_page_request(fiat=fiat, crypto=crypto, trade_type="SELL", timestamp=timestamp,
                                debug=debug)
    print(f"\n[binanceRequest] Scraping complete. Saving raw data to MongoDB...")
    return sell_data, buy_data


def ads_page_request(timestamp, fiat="BOB", crypto="USDT", trade_type="BUY", debug=False):
    """
    Scrape Binance P2P ads pages for a given fiat and crypto, and return filtered ads data.

    This function iterates through the paginated Binance P2P API, requesting ads for the specified
    fiat and crypto, and trade type (BUY/SELL). It applies filters to each ad using the filter_ad
    function and collects valid ads. Optionally, it saves raw responses for debugging.

    Args:
        timestamp (datetime): The timestamp of the request, used for snapshot filenames.
        fiat (str, optional): The fiat currency to filter ads. Defaults to "BOB".
        crypto (str, optional): The cryptocurrency to filter ads. Defaults to "USDT".
        trade_type (str, optional): The trade type, either "BUY" or "SELL". Defaults to "BUY".
        debug (bool, optional): If True, saves raw API responses for each page. Defaults to False.

    Returns:
        list or None: A list of filtered ads dictionaries, or None if the request fails.
    """
    page = 1
    # Define the request payload
    payload = {
        "fiat": fiat,
        "page": page,
        "rows": 10,
        "tradeType": trade_type,
        "asset": crypto,
        "countries": [],
        "proMerchantAds": False,
        "shieldMerchantAds": False,
        "filterType": "all",
        "periods": [],
        "additionalKycVerifyFilter": 0,
        "publisherType": None,
        "payTypes": [],
        "classifies": [
            "mass",
            "profession",
            "fiat_trade"
        ]
    }

    # Initialize the ads data list
    ads_data = []
    go_to_next_page = True

    # Loop through the pages
    while go_to_next_page:
        print(f"[binanceRequest] Processing {trade_type.lower()} ads page {page}...")
        # Update the page number in the payload
        payload["page"] = page
        # Send the POST request
        response = requests.post(url, json=payload, headers=config.USER_AGENT_HEADERS)
        # Check the response status
        if response.status_code == 200:
            # Parse the JSON response
            response_json = response.json()
            if debug:
                # Save the raw response to a file
                snapshot_saver(ads_page=response_json, source="binance", fiat=fiat, crypto=crypto,
                               snap_type=trade_type, page=page, timestamp=timestamp)
            # Check if the response contains an error message
            if response_json["message"] is None:
                # Check if the response contains exchange ads
                if response_json["total"] != 0:
                    # Extract the ads from the response
                    response_ads = response_json["data"]
                    for ad in response_ads:
                        # Extract relevant data from the ad
                        ad_dict = {
                            "username": ad["advertiser"]["nickName"],
                            "price": float(ad["adv"]["price"]),
                            "volume": float(ad["adv"]["tradableQuantity"])
                        }
                        # Prepare ad data for filtering
                        ad_conditions_data = [
                            ad_dict["volume"],
                            ad_dict["price"],
                            ad["adv"]["maxSingleTransAmount"],
                            ad["adv"]["isTradable"],
                            ad["advertiser"]["monthOrderCount"],
                            ad["advertiser"]["activeTimeInSecond"],
                            ad["advertiser"]["monthFinishRate"],
                            ad["advertiser"]["positiveRate"],
                            ad["adv"]["tradeMethods"],
                            ad["advertiser"]["nickName"]
                        ]
                        # Filter the ad and determine if we should continue to the next page
                        filter_check, go_to_next_page = filter_ad(ad_data=ad_conditions_data, fiat=fiat, crypto=crypto)
                        if filter_check:  # If it passes all the filters
                            ads_data.append(ad_dict)
                    page += 1
                else:  # Response contains no ads
                    go_to_next_page = False
            else:  # Response contains an error message
                print(f"[binanceRequest] Request failed with error message: {response_json['message']}")
                return None
        else:  # Request failed
            print(f"[binanceRequest] Request failed with status code: {response.status_code}")
            return None
    return ads_data


def snapshot_saver(ads_page, source, fiat, crypto, snap_type, page, timestamp):
    """
    Save the snapshot of ads data to a JSON file.

    This function saves the provided ads data to a JSON file. The filename is generated
    based on the timestamp, source, fiat, crypto, snap type, and page number.

    Args:
        ads_page (dict): The ads data to be saved.
        source (str): The source of the data.
        fiat (str): The fiat currency.
        crypto (str): The cryptocurrency.
        snap_type (str): The type of snapshot (e.g., BUY or SELL).
        page (int): The page number of the ads' data.
        timestamp (datetime.now()): The timestamp of the snapshot.

    Returns:
        int: 0 if the snapshot is saved successfully.
    """
    filename = f"{timestamp.strftime('%Y-%m-%dT%H.%M.%S.%f')[:-3]}_{source}_{fiat}-{crypto}_{snap_type.lower()}_page{page}.json"
    file_path = config.SNAPSHOTS_DIR / filename
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(ads_page, file, indent=4)
    return 0


if __name__ == "__main__":
    pass
