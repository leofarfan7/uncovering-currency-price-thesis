import json
from datetime import datetime, timedelta

import pandas as pd
import pytz
from tqdm import tqdm

from config import UTILS_DIR
from utils.mongo_controller import mongo_controller


def aggregate_raw_data(timestamp, fiat, sell_raw_data, buy_raw_data, raw=True, _id=None):
    """
    Aggregates and processes raw buy and sell advertisement data for a given timestamp and fiat currency.

    This function filters out blocked users (for BOB fiat), removes outliers, computes VWAP (Volume Weighted Average Price),
    quoted spread, sell and buy volumes within a threshold, and liquidity depth. The processed data is either saved to the
    database or returned as a dictionary.

    Args:
        timestamp (datetime): The timestamp for the data aggregation.
        fiat (str): The fiat currency (e.g., "BOB", "ARS").
        sell_raw_data (list): List of raw sell advertisement data (dicts).
        buy_raw_data (list): List of raw buy advertisement data (dicts).
        raw (bool, optional): If True, saves the data to the database. If False, returns the data dict. Defaults to True.
        _id (Any, optional): The document ID to use when not saving as raw. Defaults to None.

    Returns:
        dict: Aggregated and processed data including VWAP, spread, volumes, and liquidity depth.
    """
    if (not raw) and (fiat == "BOB"):
        with open(UTILS_DIR / "blocked_users.json", "r", encoding="utf-8") as f:
            blocked_users = json.load(f)["blocked_users"]
        sell_raw_data = [adv for adv in sell_raw_data if adv["username"] not in blocked_users]
        buy_raw_data = [adv for adv in buy_raw_data if adv["username"] not in blocked_users]

    sell_data_df = filter_outliers(pd.DataFrame(sell_raw_data))
    sell_data_df.index = range(1, len(sell_data_df) + 1)
    sell_data_df["position"] = sell_data_df.index
    buy_data_df = filter_outliers(pd.DataFrame(buy_raw_data))
    buy_data_df.index = range(1, len(buy_data_df) + 1)
    buy_data_df["position"] = buy_data_df.index

    # Compute VWAP
    vwap_ads_to_check = 35 if fiat == "BOB" else 70
    buy_vwap = compute_vwap(buy_data_df.head(vwap_ads_to_check).copy())
    sell_vwap = compute_vwap(sell_data_df.head(vwap_ads_to_check).copy())

    # Compute quoted spread
    spread = compute_spread(sell_vwap, buy_vwap)

    percentage_threshold = 0.01
    # Compute Sell Volume
    vwap_lower_bound = 0
    vwap_upper_bound = sell_vwap + (sell_vwap * percentage_threshold)
    filtered_sell_data_df = sell_data_df[
        (sell_data_df["price"] > vwap_lower_bound) & (sell_data_df["price"] < vwap_upper_bound)]
    sell_volume = round(filtered_sell_data_df["volume"].sum(), 2)
    # Compute Buy Volume
    vwap_lower_bound = buy_vwap - (buy_vwap * percentage_threshold)
    vwap_upper_bound = float("inf")
    filtered_buy_data_df = buy_data_df[
        (buy_data_df["price"] > vwap_lower_bound) & (buy_data_df["price"] < vwap_upper_bound)]
    buy_volume = round(filtered_buy_data_df["volume"].sum(), 2)
    # Compute Relative Volume
    # rel_buy_vol = compute_rel_vol(timestamp=timestamp, fiat=fiat, crypto=crypto, source=source,
    #                               current_volume=buy_volume, period_days=7)
    # rel_sell_vol = compute_rel_vol(timestamp=timestamp, fiat=fiat, crypto=crypto, source=source,
    #                                current_volume=sell_volume, period_days=7)

    sell_liquidity_depth = compute_liquidity_depth(sell_data_df)
    buy_liquidity_depth = compute_liquidity_depth(buy_data_df)

    data_dict = {
        "timestamp": timestamp,
        "sell_raw_data": sell_raw_data,
        "buy_raw_data": buy_raw_data,
        "sell_vwap": sell_vwap,
        "buy_vwap": buy_vwap,
        "spread": spread,
        "sell_volume": sell_volume,
        "buy_volume": buy_volume,
        "sell_liquidity_depth": sell_liquidity_depth,
        "buy_liquidity_depth": buy_liquidity_depth
    }

    if raw:
        mongo_controller.save_data(collection=f"USDT_{fiat}_Binance", data=data_dict)
    else:
        data_dict["_id"] = _id
    return data_dict


def compute_vwap(df):
    """
    Compute the Volume Weighted Average Price (VWAP) for a given DataFrame.

    Args:
        df (DataFrame): A pandas DataFrame containing 'price' and 'volume' columns.

    Returns:
        float: The computed VWAP value rounded to 2 decimal places.
    """
    # Calculate the weight as the inverse of the position
    df['weight'] = 1 / df['position']

    # Calculate the weighted price * volume
    df['weighted_price_volume'] = df['price'] * df['volume'] * df['weight']

    # Calculate the weighted volume
    df['weighted_volume'] = df['volume'] * df['weight']

    # Compute the VWAP with weights
    weighted_sum = df['weighted_price_volume'].sum()
    total_weighted_volume = df['weighted_volume'].sum()

    vwap = weighted_sum / total_weighted_volume
    return round(vwap, 2)


def compute_liquidity_depth(df):
    """
    Compute the liquidity depth for each price level in the given DataFrame.

    Args:
        df (DataFrame): A pandas DataFrame containing price and volume data.

    Returns:
        list: A list of dictionaries, each containing 'price' and 'volume' keys, representing the liquidity depth.
    """
    liquidity_depth_dict = {}
    for index, row in df.iterrows():
        liquidity_depth_dict[row["price"]] = (
                liquidity_depth_dict.get(row["price"], 0) + row["volume"]
        )

    # Convert dictionary to DataFrame
    liquidity_depth_df = pd.DataFrame(
        list(liquidity_depth_dict.items()), columns=["price", "volume"]
    ).sort_values(by="price", ascending=True)
    liquidity_depth_df["volume"] = liquidity_depth_df["volume"].round(2)

    return liquidity_depth_df.to_dict(orient="records")


def compute_rel_vol(timestamp, fiat, current_volume):
    """
    Compute the relative volume of the current period compared to the historical average.

    Args:
        timestamp (datetime): The reference timestamp for the query.
        fiat (str): The fiat currency (e.g., "BOB", "ARS").
        current_volume (float): The current volume to compare.

    Returns:
        float: The ratio of current volume to the historical average sell volume.
               Returns 1 if there is no historical data.
    """
    data_df = mongo_controller.query_data(_mode="all", collection=f"USDT_{fiat}_Binance",
                                          _filter={
                                              "timestamp": {"$gte": timestamp - timedelta(weeks=208)},
                                          }, sort=1)
    if data_df.empty:
        return 1
    avg_vol = data_df["sell_volume"].mean()
    rel_vol = current_volume / avg_vol
    return rel_vol


def compute_spread(sell_vwap, buy_vwap):
    """
    Compute the spread between the sell and buy VWAP.

    Args:
        sell_vwap (float): The sell VWAP value.
        buy_vwap (float): The buy VWAP value.

    Returns:
        float: The computed spread value.
    """
    return round((sell_vwap - buy_vwap) / ((sell_vwap + buy_vwap) / 2) * 100, 2)


def filter_outliers(df):
    """
    Filter outliers from a DataFrame based on the Interquartile Range (IQR) method.

    Args:
        df (DataFrame): A pandas DataFrame containing price data.

    Returns:
        DataFrame: A pandas DataFrame with outliers removed.
    """
    q1 = df["price"].quantile(0.25)
    q3 = df["price"].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    if not lower_bound == upper_bound:
        filtered_df = df[(df["price"] > lower_bound) & (df["price"] < upper_bound)]
    else:
        filtered_df = df
    return filtered_df


def calculate_daily_averages():
    """
    Calculate and store daily averages for various currency exchange rates and trading data.

    This function iterates over each day from January 1, 2022, to the current date (in the 'America/La_Paz' timezone),
    aggregates data from multiple MongoDB collections, computes daily averages for each relevant metric, and stores
    the results in the 'Daily_Averages' collection. It also updates the 'USD_BOB_Parallel_series' for each day
    using a smoothed curve.

    The function processes the following collections:
        - USDT_BOB_Binance
        - USDT_ARS_Binance
        - USDT_ARS_TradingView
        - USD_ARS_Parallel
        - USD_BOB_Parallel
        - USD_BOB_Official
        - USD_BOB_Tarjeta
        - USD_ARS_Official

    For each day, it:
        - Computes VWAP, spread, and volume for Binance data.
        - Handles fallback to 'USDT_BOB_Other' if Binance data is missing.
        - Aggregates open, close, high, low, and volume for TradingView data.
        - Collects parallel and official exchange rates.
        - Updates or inserts the daily average document in the database.
        - After all days are processed, updates the 'USD_BOB_Parallel_series' with a smoothed curve.

    Returns:
        None
    """
    la_paz_tz = pytz.timezone('America/La_Paz')
    start_date_lpz = la_paz_tz.localize(datetime(2022, 1, 1))
    end_date_lpz = la_paz_tz.localize(datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    num_days = (end_date_lpz - start_date_lpz).days + 1
    current_date_lpz = start_date_lpz
    for date in tqdm(range(num_days), total=num_days, desc="Processing data", unit="doc"):
        current_date_utc = current_date_lpz.astimezone(pytz.utc).replace(tzinfo=None).replace(hour=0, minute=0,
                                                                                              second=0, microsecond=0)
        daily_average_doc = {'timestamp': current_date_utc}
        start_of_day_lpz = current_date_lpz
        end_of_day_lpz = start_of_day_lpz + timedelta(days=1)

        # Process USDT_BOB_Binance and USDT_ARS_Binance
        for collection in ["USDT_BOB_Binance", "USDT_ARS_Binance"]:
            current_date_data = mongo_controller.query_data(_mode="all", collection=collection,
                                                            _filter={"timestamp": {
                                                                "$gte": start_of_day_lpz,
                                                                "$lt": end_of_day_lpz
                                                            }}, sort=1)
            if not current_date_data.empty and len(current_date_data) > 5:
                daily_average_doc[collection] = {
                    'sell_vwap': round(float(current_date_data['sell_vwap'].mean()), 2),
                    'buy_vwap': round(float(current_date_data['buy_vwap'].mean()), 2),
                    'sell_volume': round(float(current_date_data['sell_volume'].mean()),
                                         2) if 'sell_volume' in current_date_data.columns else None,
                    'buy_volume': round(float(current_date_data['buy_volume'].mean()),
                                        2) if 'buy_volume' in current_date_data.columns else None
                }
                daily_average_doc[collection]['spread'] = compute_spread(daily_average_doc[collection]['sell_vwap'],
                                                                         daily_average_doc[collection]['buy_vwap'])
                for volume in ['sell_volume', 'buy_volume']:
                    if daily_average_doc[collection][volume] == 0:
                        daily_average_doc[collection][volume] = None
            else:
                if collection == "USDT_BOB_Binance":
                    current_date_data = mongo_controller.query_data(_mode="all", collection="USDT_BOB_Other",
                                                                    _filter={"timestamp": {
                                                                        "$gte": start_of_day_lpz,
                                                                        "$lt": end_of_day_lpz
                                                                    }}, sort=1)
                    if not current_date_data.empty:
                        daily_average_doc[collection] = {
                            'sell_vwap': round(float(current_date_data['sell_price'].mean()), 2),
                            'buy_vwap': round(float(current_date_data['buy_price'].mean()), 2),
                            'sell_volume': None,
                            'buy_volume': None,
                        }
                        daily_average_doc[collection]['spread'] = compute_spread(
                            daily_average_doc[collection]['sell_vwap'],
                            daily_average_doc[collection]['buy_vwap'])
                    else:
                        daily_average_doc[collection] = None
                else:
                    daily_average_doc[collection] = None
        # Process USDT_ARS_TradingView
        if current_date_lpz.astimezone(pytz.utc) >= pytz.utc.localize(datetime(year=2023, month=5, day=1)):
            current_date_data = mongo_controller.query_data(_mode="all", collection="USDT_ARS_TradingView",
                                                            _filter={"timestamp": current_date_utc}, sort=1)
            if not current_date_data.empty:
                daily_average_doc["USDT_ARS_TradingView"] = {
                    'open': round(float(current_date_data['open'].mean()), 2),
                    'close': round(float(current_date_data['close'].mean()), 2),
                    'high': round(float(current_date_data['high'].mean()), 2),
                    'low': round(float(current_date_data['low'].mean()), 2),
                    'volume': round(float(current_date_data['volume'].sum()), 2)
                }
            else:
                daily_average_doc["USDT_ARS_TradingView"] = None
        # Process USD_ARS_Parallel
        current_date_data = mongo_controller.query_data(_mode="one", collection="USD_ARS_Parallel",
                                                        _filter={"timestamp": current_date_utc})
        if current_date_data is not None:
            daily_average_doc["USD_ARS_Parallel"] = {
                'sell_price': current_date_data['sell_price']
            }
        else:
            daily_average_doc["USD_ARS_Parallel"] = None
        # Process USD_BOB_Parallel
        sources = []
        interval = [float('inf'), float('-inf')]
        current_date_data = mongo_controller.query_data(_mode="all", collection="USD_BOB_Parallel",
                                                        _filter={
                                                            "timestamp": {
                                                                "$gte": start_of_day_lpz,
                                                                "$lt": end_of_day_lpz
                                                            },
                                                            "human_approved": True
                                                        }, sort=1)
        if not current_date_data.empty:
            for idx, row in current_date_data.iterrows():
                sources.append({'source': row['source'], 'url': row['url']})
                if row['hint_type'] == 'exact':
                    if row['quote'] < interval[0]:
                        interval[0] = row['quote']
                    if row['quote'] > interval[1]:
                        interval[1] = row['quote']
                elif row['hint_type'] == 'above':
                    if row['quote'] < interval[0]:
                        interval[0] = row['quote']
                    if (row['quote'] + 1) > interval[1]:
                        interval[1] = row['quote'] + 1
                elif row['hint_type'] == 'below':
                    if row['quote'] > interval[1]:
                        interval[1] = row['quote']
                    if (row['quote'] - 1) < interval[0]:
                        interval[0] = row['quote'] - 1
            daily_average_doc["USD_BOB_Parallel"] = {
                'quote_interval': interval,
                'sources': sources
            }
        else:
            daily_average_doc["USD_BOB_Parallel"] = None
        daily_average_doc["USD_BOB_Parallel_series"] = None
        # Process USD_BOB_Official
        daily_average_doc["USD_BOB_Official"] = {
            'sell_price': 6.96,
            'buy_price': 6.86
        }
        # Process USD_BOB_Tarjeta
        current_date_data = mongo_controller.query_data(_mode="one", collection="USD_BOB_Tarjeta",
                                                        _filter={"timestamp": current_date_utc})
        if current_date_data is not None:
            daily_average_doc["USD_BOB_Tarjeta"] = {
                'sell_price': current_date_data['price']
            }
        else:
            daily_average_doc["USD_BOB_Tarjeta"] = None
        # Process USD_ARS_Official
        current_date_data = mongo_controller.query_data(_mode="one", collection="USD_ARS_Official",
                                                        _filter={"timestamp": current_date_utc})
        if current_date_data is not None:
            daily_average_doc["USD_ARS_Official"] = {
                'open': round(float(current_date_data['open']), 2),
                'close': round(float(current_date_data['close']), 2),
                'high': round(float(current_date_data['high']), 2),
                'low': round(float(current_date_data['low']), 2),
            }
        else:
            daily_average_doc["USD_ARS_Official"] = None

        current_date_lpz = end_of_day_lpz

        # Write data to database
        exists = mongo_controller.query_data(_mode="one", collection="Daily_Averages",
                                             _filter={"timestamp": current_date_utc})
        if exists is None:
            mongo_controller.save_data(collection="Daily_Averages", data=daily_average_doc)
            # print("Inserted a new document.")
        else:
            exists_id = exists.pop('_id')
            if exists == daily_average_doc:
                # print("Existing document is equal to new document. Skipping...")
                continue
            else:
                mongo_controller.update_data(collection="Daily_Averages", _id=exists_id, data=daily_average_doc)
                # print("Updated existing document.")
    # Update USD_BOB_Parallel_series with smoothed curve values
    parallel_series = compute_bob_parallel_curve()
    for idx, value in tqdm(parallel_series.items(), total=len(parallel_series), desc="Updating parallel curve series",
                           unit="doc"):
        doc = mongo_controller.query_data(_mode='one', collection='Daily_Averages', _filter={'timestamp': idx})
        doc['USD_BOB_Parallel_series'] = value
        mongo_controller.update_data(collection='Daily_Averages', _id=doc['_id'], data=doc)


def calculate_x_period_averages(period="quarter"):
    """
    Calculate and store averages for each quarter or month based on daily averages.

    This function aggregates daily average data from the 'Daily_Averages' collection in MongoDB
    and computes period-based (quarterly or monthly) averages for various exchange rates and trading metrics.
    The results are saved or updated in the corresponding 'Quarterly_Averages' or 'Monthly_Averages' collections.

    Args:
        period (str, optional): The period for aggregation. Accepts "quarter" or "month". Defaults to "quarter".

    Returns:
        None
    """
    start_dates = []
    current_month = datetime.now().month
    if period == "quarter":
        for year in range(2022, 2025 + 1):
            for month in range(1, 12 + 1, 3):
                if year == 2025 and (month - current_month) >= 3:
                    break
                start_dates.append(datetime(year, month, 1))
    elif period == "month":
        for year in range(2022, 2025 + 1):
            for month in range(1, 12 + 1):
                if year == 2025 and month > (current_month + 1):
                    break
                start_dates.append(datetime(year, month, 1))
    while True:
        if len(start_dates) == 1:
            break
        current_date = start_dates.pop(0)
        end_of_period = (start_dates[0] - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        daily_averages = mongo_controller.query_data(_mode="all", collection="Daily_Averages",
                                                     _filter={"timestamp": {
                                                         "$gte": current_date,
                                                         "$lte": end_of_period
                                                     }})
        if period == "quarter":
            x_period_average_doc = {"quarter": str((current_date.month - 1) // 3 + 1), "year": str(current_date.year)}
        else:  # if period == "month"
            x_period_average_doc = {"month": str(current_date.month), "year": str(current_date.year)}

        for collection in daily_averages.columns:
            if collection in ["_id", "timestamp", "USD_BOB_Parallel_series"]:
                continue
            if daily_averages[collection].isnull().all():
                if collection != "USD_BOB_Parallel_series":
                    x_period_average_doc[collection] = None
                continue
            temp_df = daily_averages[collection].apply(pd.Series)
            if collection in ["USDT_BOB_Binance", "USDT_ARS_Binance"]:
                x_period_average_doc[collection] = {
                    'sell_vwap': round(float(temp_df['sell_vwap'].mean()), 2),
                    'buy_vwap': round(temp_df['buy_vwap'].mean(), 2),
                    'sell_volume': round(temp_df['sell_volume'].mean(), 2),
                    'buy_volume': round(temp_df['buy_volume'].mean(), 2)
                }
                x_period_average_doc[collection]['spread'] = compute_spread(
                    x_period_average_doc[collection]['sell_vwap'],
                    x_period_average_doc[collection]['buy_vwap'])
            elif collection in ["USDT_ARS_TradingView"]:
                x_period_average_doc[collection] = {
                    'open': round(float(temp_df['open'].iloc[0]), 2),
                    'close': round(float(temp_df['close'].iloc[-1]), 2),
                    'average': round(float((temp_df['open'].mean() + temp_df['close'].mean()) / 2), 2),
                    'high': round(float(temp_df['high'].max()), 2),
                    'low': round(float(temp_df['low'].min()), 2),
                    'volume': round(float(temp_df['volume'].mean()), 2)
                }
            elif collection in ["USD_ARS_Parallel"]:
                x_period_average_doc[collection] = {
                    'sell_price': round(float(temp_df['sell_price'].mean()), 2)
                }
            elif collection in ["USD_BOB_Parallel"]:
                sources = []
                for index, src in temp_df['sources'].items():
                    if not isinstance(src, float):
                        for individual_src in src:
                            sources.append(individual_src['url'])
                interval = pd.DataFrame()
                interval['lower_bound'] = temp_df['quote_interval'].apply(
                    lambda x: x[0] if isinstance(x, list) else None)
                interval['upper_bound'] = temp_df['quote_interval'].apply(
                    lambda x: x[1] if isinstance(x, list) else None)
                interval = interval.dropna()
                if not interval.empty:
                    lower_bound = interval['lower_bound'].min()
                    upper_bound = interval['upper_bound'].max()
                x_period_average_doc[collection] = {
                    'quote_interval': [lower_bound, upper_bound],
                    'series_average': round(float(daily_averages['USD_BOB_Parallel_series'].mean()), 2),
                    'sources': sources
                }
            elif collection in ["USD_BOB_Official"]:
                x_period_average_doc[collection] = {
                    'sell_price': 6.96,
                    'buy_price': 6.86
                }
            elif collection in ["USD_BOB_Tarjeta"]:
                x_period_average_doc[collection] = {
                    'sell_price': round(float(temp_df['sell_price'].mean()), 2)
                }
            elif collection in ["USD_ARS_Official"]:
                x_period_average_doc[collection] = {
                    'open': round(float(temp_df['open'].mean()), 2),
                    'close': round(float(temp_df['close'].mean()), 2),
                    'average': round(float((temp_df['open'].mean() + temp_df['close'].mean()) / 2), 2),
                    'high': round(float(temp_df['high'].mean()), 2),
                    'low': round(float(temp_df['low'].mean()), 2)
                }

        if period == "quarter":
            exists = mongo_controller.query_data(_mode="one", collection="Quarterly_Averages",
                                                 _filter={"quarter": x_period_average_doc["quarter"],
                                                          "year": x_period_average_doc["year"]})
        else:  # if period == "month"
            exists = mongo_controller.query_data(_mode="one", collection="Monthly_Averages",
                                                 _filter={"month": x_period_average_doc["month"],
                                                          "year": x_period_average_doc["year"]})

        if exists is None:
            if period == "quarter":
                mongo_controller.save_data(collection="Quarterly_Averages", data=x_period_average_doc)
            else:  # if period == "month"
                mongo_controller.save_data(collection="Monthly_Averages", data=x_period_average_doc)
        else:
            exists_id = exists.pop('_id')
            if exists == x_period_average_doc:
                continue
            else:
                if period == "quarter":
                    mongo_controller.update_data(collection="Quarterly_Averages", _id=exists_id,
                                                 data=x_period_average_doc)
                else:  # if period == "month"
                    mongo_controller.update_data(collection="Monthly_Averages", _id=exists_id,
                                                 data=x_period_average_doc)


def filter_ad(ad_data, fiat, crypto):
    """
    Filter advertisements based on specific conditions for different fiat currencies, cryptocurrencies, and sources.

    Args:
        ad_data (list): A list containing raw advertisement data.
        fiat (str): The fiat currency to query.
        crypto (str): The cryptocurrency to query.

    Returns:
        tuple: A tuple containing:
            - bool: True if the advertisement meets any of the conditions, False otherwise.
            - bool: True if the process should go to the next page, False otherwise.

    Filtering logic:
    - For ("BOB", "USDT"):
        * Loads blocked users from blocked_users.json.
        * Sets go_to_next_page if price (ad_data[1]) is above 6.96.
        * Applies conditions on volume, price, transaction amount, tradability, order count, last seen, finish rate, positive rate, and username.
        * Additionally, if any trade method is "Banco Fassil" or "Tigo Money", the ad is filtered out.
    - For ("ARS", "USDT"):
        * Always sets go_to_next_page to True.
        * Applies conditions on volume, tradability, order count, last seen, finish rate, and positive rate.
    - For all other cases:
        * Always filters out the ad (ad_conditions = [True]).
        * Sets go_to_next_page to False.
    """
    match fiat, crypto:
        case "BOB", "USDT":
            with open(UTILS_DIR / "blocked_users.json", "r", encoding="utf-8") as f:
                blocked_users = json.load(f)["blocked_users"]
            go_to_next_page = ad_data[1] > 6.96
            ad_conditions = [
                ad_data[0] < 100,  # Volume USDT
                ad_data[1] <= 6.96,  # Price BOB
                float(ad_data[2]) <= 100,  # Max Single Transaction Amount
                ad_data[3] is False,  # Is Tradable
                int(ad_data[4]) < 20,  # Month Order Count
                int(ad_data[5]) > 43200 if ad_data[5] is not None else True,  # Last Seen in Seconds (12h)
                float(ad_data[6]) < 0.75,  # Month Finish Rate
                float(ad_data[7]) < 0.95 if ad_data[7] is not None else True,  # Positive Rate
                ad_data[8] in blocked_users  # Username
            ]
            for trade_method in ad_data[8]:
                if trade_method["tradeMethodName"] in ["Banco Fassil", "Tigo Money"]:
                    ad_conditions.append(True)
        case "ARS", "USDT":
            go_to_next_page = True
            ad_conditions = [
                ad_data[0] < 50,  # Volume USDT
                ad_data[3] is False,  # Is Tradable
                int(ad_data[4]) < 50,  # Month Order Count
                int(ad_data[5]) > 43200 if ad_data[5] is not None else True,  # Last Seen in Seconds (12h)
                float(ad_data[6]) < 0.75,  # Month Finish Rate
                float(ad_data[7]) < 0.95 if ad_data[7] is not None else True  # Positive Rate
            ]
        case _:  # Default case
            ad_conditions = [True]
            go_to_next_page = False
    return not any(ad_conditions), go_to_next_page


def review_processed_data(fiat):
    """
    Review and reprocess previously stored raw data for a given fiat currency.

    This function iterates over all time buckets in the MongoDB time-series collection for the specified fiat,
    retrieves the raw documents within each bucket, re-aggregates the data using `aggregate_raw_data`, and
    replaces the old documents with the newly processed ones.

    Args:
        fiat (str): The fiat currency (e.g., "BOB", "ARS") to process.

    Returns:
        None
    """
    bucket_collection = f"system.buckets.USDT_{fiat}_Binance"
    total_buckets = mongo_controller.db[bucket_collection].count_documents({})
    for bucket in tqdm(mongo_controller.db[bucket_collection].find().sort({"_id": -1}), total=total_buckets,
                       desc="Processing data", unit="bucket"):
        min_ts = bucket["control"]["min"]["timestamp"]
        max_ts = bucket["control"]["max"]["timestamp"]
        time_range = {"timestamp": {"$gte": min_ts, "$lte": max_ts}}
        raw_docs = mongo_controller.query_data(_mode="all", collection=f"USDT_{fiat}_Binance",
                                               _filter=time_range, _datatype="cursor",
                                               projection={"_id": 1, "timestamp": 1, "sell_raw_data": 1,
                                                           "buy_raw_data": 1})
        raw_docs = list(raw_docs)
        new_batch = []
        for row in raw_docs:
            new_batch.append(aggregate_raw_data(timestamp=row["timestamp"], fiat=fiat,
                                                sell_raw_data=row["sell_raw_data"], buy_raw_data=row["buy_raw_data"],
                                                raw=False, _id=row["_id"]))
        if new_batch:
            mongo_controller.db[f"USDT_{fiat}_Binance"].delete_many(time_range)
            mongo_controller.db[f"USDT_{fiat}_Binance"].insert_many(new_batch)


def compute_bob_parallel_curve():
    """
    Compute a smoothed time series curve for the BOB parallel exchange rate.

    This function retrieves daily average documents from the 'Daily_Averages' collection,
    extracts the 'USD_BOB_Parallel' quote intervals, and calculates the mean of the lower
    and upper bounds for each day. It then interpolates missing values linearly and applies
    a rolling mean smoothing (3 iterations, window size 5) to produce a smooth curve.

    Returns:
        pandas.Series: A time-indexed series representing the smoothed BOB parallel exchange rate.
    """
    df = mongo_controller.query_data(_mode='all', collection='Daily_Averages')
    df = df[['timestamp', 'USD_BOB_Parallel']]
    df['USD_BOB_Parallel'] = df['USD_BOB_Parallel'].apply(
        lambda x: x['quote_interval'] if isinstance(x, dict) else [None, None])
    df['lower_bound'] = df['USD_BOB_Parallel'].apply(lambda x: x[0])
    df['upper_bound'] = df['USD_BOB_Parallel'].apply(lambda x: x[1])
    df.drop(columns=['USD_BOB_Parallel'], inplace=True)
    df = df[df['timestamp'] > '2023-02-08'].copy()
    df.sort_values('timestamp')
    df.set_index('timestamp', inplace=True)

    anchors = df[['lower_bound', 'upper_bound']].mean(axis=1)
    curve = anchors.interpolate(method='linear')
    smoothed_curve = curve.copy()
    for _ in range(3):  # repeat smoothing 3 times
        smoothed_curve = smoothed_curve.rolling(window=5, center=True, min_periods=1).mean()

    return smoothed_curve


if __name__ == "__main__":
    # For testing purposes only
    # timestamp = datetime.fromisoformat("2025-04-24T16:20:00.000+00:00")
    # data = mongo_controller.query_data(_mode="one", collection="USDT_BOB_Binance",
    #                                    _filter={"timestamp": timestamp})
    # print(aggregate_raw_data(timestamp=timestamp, fiat="BOB",
    #                          sell_raw_data=data["sell_raw_data"], buy_raw_data=data["buy_raw_data"],
    #                          raw=False, _id=data["_id"]))

    # print("[data_processing] Starting a review of processed data...")
    # review_processed_data(fiat="BOB")
    print("[data_processing] Starting daily averages calculation...")
    calculate_daily_averages()
    # print("[data_processing] Starting quarterly averages calculation...")
    # calculate_x_period_averages(period="quarter")
    # print("[data_processing] Starting monthly averages calculation...")
    # calculate_x_period_averages(period="month")
