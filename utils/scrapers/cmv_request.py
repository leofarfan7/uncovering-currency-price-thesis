from datetime import datetime

import pandas as pd
import pdfplumber
import requests

import config
from utils.mongo_controller import mongo_controller

url = "https://backportal.bmsc.com.bo:1443/api/bmsc-portal/reports/547/file"


def cmv_request():
    """
    Downloads a PDF report from a specified URL, extracts table data from the PDF,
    processes the data into a DataFrame, calculates additional fields, and saves
    the results to a MongoDB collection if not already present.

    Steps:
    1. Download the PDF file using a GET request with custom headers.
    2. Save the PDF to a directory specified in the config.
    3. Extract the first table from each page of the PDF using pdfplumber.
    4. Convert the extracted data into a pandas DataFrame with columns 'date' and 'cmv'.
    5. Parse the 'date' column and calculate the 'price' column.
    6. For each row, check if the entry exists in the MongoDB collection 'USD_BOB_Tarjeta'.
       If not, save the new data.
    7. Print status messages throughout the process.

    Returns:
        int: 0 on successful completion.
    """
    response = requests.get(url, headers=config.USER_AGENT_HEADERS)
    filename = config.CMV_DIR / f"{datetime.now().strftime("%Y-%m-%d")}_CMV_BMSC.pdf"
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        print("[cmv_request] PDF downloaded successfully.")
    else:
        print(f"[cmv_request] Failed to download PDF: {response.status_code}")

    with pdfplumber.open(filename) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            table = page.extract_tables()[0][1:]
            table = map(lambda row: [row[0], float(row[3])], table)
            table_df = pd.DataFrame(table, columns=["date", "cmv"])

    table_df["date"] = pd.to_datetime(table_df["date"], format="%d/%m/%Y")
    table_df["price"] = round(((table_df["cmv"] / 100) + 1) * 6.97, 2)

    for index, row in table_df.iterrows():
        exists = mongo_controller.query_data(_mode="one", collection="USD_BOB_Tarjeta",
                                             _filter={"timestamp": row["date"]})
        if not exists:
            mongo_controller.save_data(collection="USD_BOB_Tarjeta",
                                       data={
                                           "timestamp": row["date"],
                                           "cmv": row["cmv"],
                                           "price": row["price"]
                                       })

    print("[cmv_request] Data has been saved successfully.")
    return 0


if __name__ == "__main__":
    cmv_request()
