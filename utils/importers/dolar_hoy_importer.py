from datetime import datetime

import pandas as pd
from tqdm import tqdm

import config
from utils.mongo_controller import mongo_controller

DOLAR_HOY_DATA_DIR = config.DATA_DIR / "dolar_hoy"
data_filename = "data_dolarhoycom10042025.csv"


def dolar_hoy_importer():
    new_data_counter = 0
    data = pd.read_csv(DOLAR_HOY_DATA_DIR / data_filename)
    for index, row in tqdm(data.iterrows(), total=data.shape[0], desc="Importing data", unit="record"):
        date = datetime.strptime(row["category"], "%a %b %d %Y")
        price = row["valor"]
        # Check if the record already exists in the database
        existing_record = mongo_controller.query_data(_mode="one",
                                                      collection="USD_ARS_Parallel",
                                                      _filter={
                                                          "timestamp": date
                                                      })
        if not existing_record:
            mongo_controller.save_data(collection="USD_ARS_Parallel",
                                       data={
                                           "timestamp": date,
                                           "metadata": {
                                               "fiat": "ARS",
                                               "source": "dolar_hoy"
                                           },
                                           "sell_price": price
                                       })
            new_data_counter += 1
    print(f"[dolar_hoy_importer] {new_data_counter} new records have been inserted.")


if __name__ == "__main__":
    dolar_hoy_importer()
