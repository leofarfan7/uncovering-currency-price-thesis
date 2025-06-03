from datetime import datetime, timedelta

from tqdm import tqdm

from config import DBCONFIG, AI_MODE
from utils.llm_processing import LLMProcessing
from utils.mongo_controller import mongo_controller
from utils.scrapers.newspapers.scraper_master import scraper_master
from utils.services import highlight_numbers

newspapers = ["el_deber", "el_diario", "los_tiempos", "red_uno", "economy", "ahoradigital", "oxigeno", "opinion",
              "fides", "erbol", "brujula"]


def newspaper_scraper(debug=False):
    """
    Scrapes newspaper data based on the current configuration state.

    - For each newspaper in the `newspapers` list:
        - If the initial complete scrape is required (`from_zero` is True), performs a full scrape from a fixed start date.
        - If a partial scrape is required (`from_zero` is "partial"), resumes scraping from the same fixed start date.
        - Otherwise, performs a regular update by determining the latest timestamp in the database and scraping new data since then.
    - Updates the configuration state after an initial complete scrape.
    - Calls `scraper_master` for each newspaper with the appropriate timestamp limit.

    Args:
        debug (bool): If True, enables debug mode for the scraper.
    """
    initial_complete_scrape = DBCONFIG.get_config(setting="newspaper_initial_complete_scrape")
    for newspaper in newspapers:
        from_zero = initial_complete_scrape.get(newspaper, True)
        if from_zero is True:  # Initial complete scrape
            print(f"\n[newspaper_processing] Starting initial complete scrape for {newspaper}...")
            DBCONFIG.update_config("newspaper_initial_complete_scrape", {newspaper: "partial"})
            timestamp_limit = datetime(2022, 9, 1)
        elif from_zero == "partial":  # Partial scrape
            print(f"\n[newspaper_processing] Resuming partial scrape for {newspaper}...")
            timestamp_limit = datetime(2022, 9, 1)
        else:  # Regular scrape
            print(f"\n[newspaper_processing] Updating {newspaper} data...")
            timestamp_limit = mongo_controller.query_data(_mode="all",
                                                          collection="USD_BOB_Parallel",
                                                          _filter={"source": newspaper},
                                                          sort=-1,
                                                          limit=10)
            timestamp_limit = timestamp_limit["timestamp"].max() - timedelta(days=2)
            print(f"Timestamp Limit: {timestamp_limit}")
        scraper_master(source=newspaper, timestamp_limit=timestamp_limit, debug=debug)


def newspaper_llm_processing():
    """
    Processes newspaper articles in two LLM stages:

    1. First-stage processing:
        - Fetches articles from the 'USD_BOB_Parallel' collection that have not been processed in the first stage.
        - For each article, uses LLMProcessing to detect relevant information.
        - Updates the article as first-stage processed and sets the second-stage flag based on detection result.

    2. Second-stage processing:
        - Fetches articles that have not been processed in the second stage.
        - For each article, uses LLMProcessing to extract 'hint_type' and 'quote'.
        - Updates the article as second-stage processed and stores the extracted information.

    Prints progress and summary information for both stages.
    """
    print("\n[newspaper_processing] Starting newspaper LLM first-stage processing...")
    total_processed_articles = 0
    articles = mongo_controller.query_data(_mode="all", collection="USD_BOB_Parallel",
                                           _filter={"first_stage_processed": False}, sort=1)
    llm_processing = LLMProcessing(mode=AI_MODE)
    for idx, article in tqdm(articles.iterrows(), total=len(articles), unit="article", desc="Analyzing articles"):
        result = llm_processing.process_article(article=article, _mode="detect")
        # print(f"Result: {result}")
        if result is True:
            second_stage = False
        else:
            second_stage = None
        mongo_controller.update_data(collection="USD_BOB_Parallel",
                                     _id=article["_id"],
                                     data={"first_stage_processed": True, "second_stage_processed": second_stage})
    if len(articles) > 0:
        print(f"[newspaper_processing] Successfully processed {len(articles)} articles in the first stage.")
    else:
        print("[newspaper_processing] No new articles to process in the first stage.")
    total_processed_articles += len(articles)
    print("\n[newspaper_processing] Starting newspaper LLM second-stage processing...")
    articles = mongo_controller.query_data(_mode="all", collection="USD_BOB_Parallel",
                                           _filter={"second_stage_processed": False}, sort=1)
    for idx, article in tqdm(articles.iterrows(), total=len(articles), unit="article", desc="Analyzing articles"):
        hint_type, quote = llm_processing.process_article(article=article, _mode="extract")
        mongo_controller.update_data(collection="USD_BOB_Parallel",
                                     _id=article["_id"],
                                     data={"second_stage_processed": True,
                                           "hint_type": hint_type,
                                           "quote": quote,
                                           "human_approved": None})
    if len(articles) > 0:
        print(f"[newspaper_processing] Successfully processed {len(articles)} articles in the second stage.")
    else:
        print("[newspaper_processing] No new articles to process in the second stage.")
    total_processed_articles += len(articles)
    print(f"\n[newspaper_processing] Processed {total_processed_articles} articles in total.")


def newspaper_reviewing():
            """
            Allows manual review and approval of newspaper articles processed by the LLM.

            - Fetches articles from the 'USD_BOB_Parallel' collection that have completed second-stage processing
              but have not yet been human approved.
            - Iterates through each article, displaying its content and extracted information.
            - Every 10 articles, prompts the user to continue or stop reviewing.
            - For each article, the user can:
                - Approve the article as correct.
                - Reject the article and clear the exchange rate.
                - Modify the extracted information (exchange rate, hint type, timestamp) and approve.
            - Updates the article in the database based on the user's input.
            - Prints a summary of the review session.

            User Inputs:
                - "y": Approve the article.
                - "n": Reject the article.
                - "m": Modify the extracted information.
            """
            print("\n[newspaper_processing] Starting manual newspaper review...")
            articles = mongo_controller.query_data(_mode="all", collection="USD_BOB_Parallel",
                                                   _filter={"second_stage_processed": True, "human_approved": None}, sort=1)
            total_articles = len(articles)
            print(f"[newspaper_processing] Found {total_articles} articles to review.")
            counter = 0
            for idx, article in articles.iterrows():
                counter += 1
                if counter % 10 == 0:
                    user_continue = input(
                        f"\n[newspaper_processing] Evaluated {counter}/{total_articles} articles. Continue? (y/n): ")
                    if user_continue == "n":
                        break
                print(f"\n[newspaper_processing] Exec {counter} | Evaluating article #{article['_id']}")
                print(f"Title: {article['title']}")
                print(f"Timestamp: {article['timestamp']}")
                print(f"Source: {article['source']}")
                print("-----\n")
                print(highlight_numbers(article['content'].strip()))
                print("\n-----")
                print(f"Hint Type: {article['hint_type']}")
                print(f"Quote: {article['quote']}")
                if article.get('exchange_rate', None):
                    print(f"Previously found exchange rate: {article['exchange_rate']}")
                while True:
                    human_eval = input("Approve (yes[y]/no[n]/modify[m]): ")
                    if human_eval == "y":
                        mongo_controller.update_data(collection="USD_BOB_Parallel", _id=article["_id"],
                                                     data={"human_approved": True})
                        break
                    elif human_eval == "n":
                        mongo_controller.update_data(collection="USD_BOB_Parallel", _id=article["_id"],
                                                     data={"exchange_rate": None, "human_approved": False})
                        break
                    elif human_eval == "m":
                        new_quote = float(input(f"Corrected exchange rate [{article['quote']}]: ") or article['quote'])
                        new_hint_type = input(f"Corrected hint type (exact/above/below) [{article['hint_type']}]: ") or article[
                            'hint_type']
                        new_timestamp = input(f"Corrected timestamp (YYYY-MM-DD)[{article['timestamp']}]: ") or article[
                            'timestamp']
                        if type(new_timestamp) is str:
                            new_timestamp = datetime.strptime(new_timestamp.strip(), "%Y-%m-%d")
                        mongo_controller.update_data(collection="USD_BOB_Parallel", _id=article["_id"],
                                                     data={"timestamp": new_timestamp, "quote": new_quote,
                                                           "hint_type": new_hint_type, "human_approved": True})
                        break
                    else:
                        print("Invalid input. Please try again.")
            if total_articles > 0:
                print(f"\n[newspaper_processing] Reviewed {counter}/{len(articles)} articles.")


if __name__ == "__main__":
    newspaper_scraper(debug=True)
    newspaper_llm_processing()
    newspaper_reviewing()
