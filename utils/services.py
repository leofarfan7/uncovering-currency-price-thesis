import json
import os
import re
import time
from datetime import datetime, timedelta

import requests

from config import BASE_DIR, DATA_DIR, SNAPSHOTS_DIR, GRAPHS_DIR, LIQUIDITY_DEPTH_DIR, \
    TWENTY_FOUR_HOURS_PRICE_DIR, ONE_WEEK_PRICE_DIR, TWO_WEEKS_PRICE_DIR, ALL_TIME_PRICE_DIR, BI_HOUR_PRICE_DIR, CMV_DIR
from config import RECORD_INTERVAL

"""
This module contains various functions, including functions to check if certain services are running.
"""


def check_folder_structure():
    """
    Check and create the required folder structure.

    This function ensures that all necessary directories exist. If a directory does not exist,
    it will be created and a message will be printed indicating the creation of the directory.
    """
    required_dirs = [DATA_DIR, SNAPSHOTS_DIR, GRAPHS_DIR, LIQUIDITY_DEPTH_DIR,
                     ONE_WEEK_PRICE_DIR, TWO_WEEKS_PRICE_DIR, TWENTY_FOUR_HOURS_PRICE_DIR, BI_HOUR_PRICE_DIR,
                     ALL_TIME_PRICE_DIR, CMV_DIR]
    for directory in required_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"[services] Directory {directory} created.")


def load_settings():
    """
    Load settings from a JSON file.

    This function reads the settings from a JSON file located at BASE_DIR/settings.json
    and returns the settings as a dictionary.

    Returns:
    dict: A dictionary containing the settings loaded from the JSON file.
    """
    with open(BASE_DIR / "settings.json", "r") as settings_file:
        settings = json.load(settings_file)
    return settings


def sleep_until_next_iteration():
    """
    Wait until the next multiple of [interval] minutes.
    """
    now = datetime.now()
    next_x_minute = now + timedelta(minutes=(RECORD_INTERVAL - now.minute % RECORD_INTERVAL))
    next_x_minute = next_x_minute.replace(second=0, microsecond=0)
    sleep_duration = (next_x_minute - now).total_seconds()
    print(f"[main] Sleeping for {int(sleep_duration // 60)} minutes, {int(sleep_duration % 60)} seconds.")
    time.sleep(sleep_duration)


def check_ollama():
    """
    Check if the Ollama service is running on localhost:11434.

    Sends a GET request to the Ollama service endpoint.
    Returns 0 if the service is running (HTTP 200), otherwise returns 1.

    Returns:
        int: 0 if Ollama is running, 1 if not or if a connection error occurs.
    """
    try:
        response = requests.get("http://localhost:11434")
        if response.status_code == 200:
            return 0
        else:
            raise ConnectionError
    except (
            requests.exceptions.RequestException, requests.exceptions.ConnectionError, ConnectionError,
            ConnectionRefusedError):
        return 1


def extract_json_response(response):
    """
    Attempt to extract and parse a JSON object from a string response.

    Tries to load the response directly as JSON. If that fails, uses a regular expression
    to extract the first JSON object found in the string and attempts to parse it.

    Args:
        response (str): The string response potentially containing JSON.

    Returns:
        dict or None: The parsed JSON object as a dictionary if successful, otherwise None.
    """
    try:
        data = json.loads(response)
        return data
    except json.JSONDecodeError:
        # Attempt to extract JSON using regex
        match = re.search(r'({.*})', response, re.DOTALL)
        if match:
            try:
                json_str = match.group(1)
                data = json.loads(json_str)
                return data
            except json.JSONDecodeError:
                return None
        return None


def highlight_numbers(text):
    """
    Returns the input string with all numbers (integers and floats) highlighted in bold red.

    ANSI escape code:
      - \033[1;31m  : Starts bold red text.
      - \033[0m     : Resets formatting to default.
    """
    # This regex matches:
    #   - an optional sign (- or +),
    #   - either a float (digits optionally before a decimal point but required after it)
    #     or an integer.
    # It uses negative lookbehind and lookahead to ensure that the number is not part of a larger word.
    pattern = re.compile(r'(?<![\w])([-+]?(?:\d*\.\d+|\d+))(?![\w])')

    # ANSI escape codes for bold red text and reset.
    bold_red = "\033[1;31m"
    reset = "\033[0m"

    # The lambda function replaces each match with the same text wrapped in ANSI codes.
    highlighted_text = pattern.sub(lambda match: f"{bold_red}{match.group(0)}{reset}", text)
    return highlighted_text


if __name__ == "__main__":
    check_folder_structure()
