from datetime import datetime, time as datetime_time
from datetime import timezone

from requests.exceptions import RequestException, HTTPError

from config import RECORD_INTERVAL, DOLAR_HOY_URL
from utils.data_processing import aggregate_raw_data
from utils.data_processing import calculate_daily_averages, calculate_x_period_averages
from utils.newspaper_processing import newspaper_scraper
from utils.scrapers.binance_request import binance_request
from utils.scrapers.cmv_request import cmv_request
from utils.scrapers.newspapers.dolar_hoy_scraper import dolar_hoy_scraper
from utils.scrapers.tradingview_request import tradingview_request
from utils.services import check_folder_structure, sleep_until_next_iteration, load_settings


def main(debug=False):
    print("\nWelcome to bolivian_blue.\n")
    print("Starting execution...\n")

    start_time = datetime_time(7, 00)
    end_time = datetime_time(23, 59)

    check_folder_structure()
    settings = load_settings()
    downloaded_cmv_data = False
    retrieved_dolar_hoy_data = False

    while True:
        current_time = datetime.now().time()
        print(f"[main] Current time: {current_time}")
        try:
            if start_time <= current_time <= end_time:
                if current_time.minute % RECORD_INTERVAL == 0:
                    timestamp = datetime.now(timezone.utc)
                    if timestamp.hour in range(13, 23) and timestamp.minute == 0:
                        if not retrieved_dolar_hoy_data and timestamp.hour in range(17, 23):
                            try:
                                print("\n[main] Requesting DolarHoy data...")
                                success = dolar_hoy_scraper(url=DOLAR_HOY_URL)
                            except Exception as e:
                                print(f"[main] DolarHoy request failed: {e}")
                                success = False
                            if success:
                                retrieved_dolar_hoy_data = True
                        if not downloaded_cmv_data:
                            try:
                                print("\n[main] Requesting CMV data...")
                                success = cmv_request()
                            except Exception as e:
                                print(f"[main] CMV request failed: {e}")
                                success = False
                            if success:
                                downloaded_cmv_data = True
                    for fiat in ["BOB", "ARS"]:
                        crypto = "USDT"
                        print(f"\n[main] Requesting Binance data for {crypto}/{fiat}...")
                        try:
                            sell_data, buy_data = binance_request(timestamp=timestamp, fiat=fiat, crypto=crypto,
                                                                  debug=debug)
                        except RequestException:
                            print("[main] Connection error. Retrying in ~5 minutes.")
                            sleep_until_next_iteration()
                            continue
                        print(f"[main] Processing data for {fiat}/{crypto}...")
                        aggregate_raw_data(timestamp=timestamp,
                                           fiat=fiat,
                                           sell_raw_data=sell_data,
                                           buy_raw_data=buy_data)
                        print(f"[main] Data has been processed successfully for {crypto}/{fiat}.")

                    sleep_until_next_iteration()
                else:
                    sleep_until_next_iteration()
            else:
                print("\n[main] Evaluating TradingView data...")
                try:
                    tradingview_request(settings["tradingview_symbols"])
                    print("[main] Extracted TradingView data successfully.")
                except Exception as e:
                    print(f"[main] TradingView request failed: {e}")
                    print("[main] Skipping TradingView data extraction.")

                print("\n[main] Updating daily averages...")
                calculate_daily_averages()
                print("[main] Updating monthly averages...")
                calculate_x_period_averages(period="month")
                print("[main] Updating quarterly averages...")
                calculate_x_period_averages(period="quarter")
                print("[main] Daily averages have been calculated successfully.")

                print("\n[main] Scraping newspapers for new articles...")
                newspaper_scraper()
                print("[main] Scraped new articles successfully.")

                print("\nTime outside working hours. Closing program...")
                return 0
        except ConnectionError or HTTPError:
            print("[main] Connection error. Retrying in ~5 minutes.")
            sleep_until_next_iteration()


if __name__ == "__main__":
    main(debug=False)
