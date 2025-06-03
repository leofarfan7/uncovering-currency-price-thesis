import os
from datetime import datetime

import config


def snapshot_saver(html_content, _type, number, source):
    """
    Saves the provided HTML content as a snapshot file.

    Args:
        html_content (str): The HTML content to save.
        _type (str): The type of snapshot, either "article_page" or "article".
        number (int or str): A unique identifier for the snapshot.
        source (str): The source name, used as a subdirectory.

    Raises:
        Exception: If the provided _type is not supported.

    The file is saved in the directory specified by config.SNAPSHOTS_DIR/source,
    with a filename based on the current timestamp, type, and number.
    """
    # Save HTML code to file, depending on whether it's a group or post snapshot
    if _type == "article_page":
        filename = f"{datetime.now().strftime('%m-%dT%H.%M.%S.%f')[:-3]}_art_page_{number}.html"
    elif _type == "article":
        filename = f"{datetime.now().strftime('%m-%dT%H.%M.%S.%f')[:-3]}_article_{number}.html"
    else:
        raise Exception("\n[seleniumScraper][snapshotSaver] Content type is wrong.")

    folder_path = config.SNAPSHOTS_DIR / source
    file_path = folder_path / filename

    if not os.path.exists(folder_path):  # Create folder in case it doesn't exist
        os.mkdir(folder_path)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(html_content)
