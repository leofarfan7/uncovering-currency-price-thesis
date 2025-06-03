from datetime import datetime

from tqdm import tqdm
from tvDatafeed import TvDatafeed, Interval

from config import DBCONFIG
from utils.mongo_controller import mongo_controller

tv_username = DBCONFIG.get_config("TRADINGVIEW_CREDENTIALS")["USERNAME"]
tv_password = DBCONFIG.get_config("TRADINGVIEW_CREDENTIALS")["PASSWORD"]


def tradingview_request(exchange_data):
    """
    Fetches historical trading data from TradingView for the given exchange data and stores it in MongoDB.

    Args:
        exchange_data (dict): A dictionary where keys are symbols (e.g., "USDARS", "USDT_ARS") and values are lists of
            exchange information dicts. Each exchange dict should contain at least:
                - "exchange": The exchange name (str)
                - "date_available": The earliest date data is available (str, format "%Y-%m-%d")

    Workflow:
        - For each symbol and exchange, determines the number of new bars (days) to fetch.
        - Requests historical data from TradingView using tvDatafeed.
        - For each record, checks if it already exists in the MongoDB collection.
        - If not, inserts the new record into the appropriate collection.
        - Handles both "USDARS" and "USDT_ARS" symbols with their respective MongoDB collections and metadata.
        - Limits the number of bars fetched to 5000 if necessary.
        - Skips records for the current day.
        - Closes the TradingView websocket connection after processing.
    """
    tv = TvDatafeed(username=tv_username, password=tv_password)
    now = datetime.now()
    for symbol in exchange_data:
        for exchange in exchange_data[symbol]:
            if symbol == "USDARS":
                db_data_df = mongo_controller.query_data(_mode="all", collection="USD_ARS_Official", sort=1)
            else:  # USDT_ARS
                db_data_df = mongo_controller.query_data(_mode="all",
                                                         collection="USDT_ARS_TradingView",
                                                         _filter={
                                                             "metadata.fiat": "ARS",
                                                             "metadata.crypto": "USDT",
                                                             "metadata.source": exchange["exchange"].lower()
                                                         },
                                                         sort=1)

            if not db_data_df.empty:
                last_timestamp = db_data_df["timestamp"].max()
                n_bars = (now - last_timestamp).days + 1
            else:
                n_bars = (now - datetime.strptime(exchange["date_available"], "%Y-%m-%d")).days
                if n_bars > 5000:
                    n_bars = 5000
            if n_bars <= 0:
                print(f"[tradingview_request] No new data available for {symbol} from {exchange['exchange']}.")
                continue
            print(
                f"[tradingview_request] Requesting {n_bars} days of data for {symbol} from {exchange['exchange']}.")
            data_df = tv.get_hist(symbol=symbol, exchange=exchange["exchange"], interval=Interval.in_daily,
                                  n_bars=n_bars)
            # print(data_df)
            # input("Press Enter to continue...")
            for index, row in tqdm(data_df.iterrows(), total=data_df.shape[0], desc="Importing data",
                                   unit="record"):
                timestamp = index.to_pydatetime().replace(hour=0)
                if (now - timestamp).days == 0:
                    continue
                data_dict = row.to_dict()
                data_dict.pop("symbol")
                if symbol == "USDARS":
                    data_doc = {
                        "timestamp": timestamp,
                        "metadata": {
                            "source": exchange["exchange"].lower()
                        }
                    }
                else:  # USDT_ARS
                    data_doc = {
                        "timestamp": timestamp,
                        "metadata": {
                            "fiat": "ARS",
                            "crypto": "USDT",
                            "source": exchange["exchange"].lower()
                        }
                    }
                for key, value in data_dict.items():
                    data_doc[key] = value
                if symbol == "USDARS":
                    # Check if the record already exists in the database
                    existing_record = mongo_controller.query_data(_mode="one",
                                                                  collection="USD_ARS_Official",
                                                                  _filter={
                                                                      "timestamp": timestamp,
                                                                      "metadata.source": exchange[
                                                                          "exchange"].lower()
                                                                  })
                    if not existing_record:
                        mongo_controller.save_data(collection="USD_ARS_Official", data=data_doc)
                else:  # USDT_ARS
                    # Check if the record already exists in the database
                    existing_record = mongo_controller.query_data(_mode="one",
                                                                  collection="USDT_ARS_TradingView",
                                                                  _filter={
                                                                      "timestamp": timestamp,
                                                                      "metadata.source": exchange[
                                                                          "exchange"].lower()
                                                                  })
                    if not existing_record:
                        mongo_controller.save_data(collection="USDT_ARS_TradingView", data=data_doc)
    tv.ws.close()
    del tv


if __name__ == "__main__":
    from utils.services import load_settings

    settings = load_settings()
    tradingview_request(settings["tradingview_symbols"])
