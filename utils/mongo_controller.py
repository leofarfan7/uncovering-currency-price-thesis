import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import config


class MongoController:
    """
    Controller class for managing MongoDB operations, including connection,
    collection creation, and CRUD operations for the 'bolivian_blue_db' database.
    """

    def __init__(self):
        """
        Initialize the MongoController by connecting to MongoDB, checking the connection,
        and ensuring required collections exist.
        """
        self.client = MongoClient(host=config.MONGO_HOST, port=config.MONGO_PORT)
        self.is_running()
        self.db = self.client.bolivian_blue_db
        self.create_collection(collection_name="config", collection_type="default")
        self.create_collection(collection_name="USDT_BOB_Binance", collection_type="timeseries")
        self.create_collection(collection_name="USDT_BOB_Other", collection_type="default")
        self.create_collection(collection_name="USDT_ARS_Binance", collection_type="timeseries")
        self.create_collection(collection_name="USDT_ARS_TradingView", collection_type="timeseries")
        self.create_collection(collection_name="USD_BOB_Parallel", collection_type="default")
        self.create_collection(collection_name="USD_ARS_Parallel", collection_type="timeseries")
        self.create_collection(collection_name="USD_ARS_Official", collection_type="timeseries")
        self.create_collection(collection_name="Daily_Averages", collection_type="default")
        self.create_collection(collection_name="Monthly_Averages", collection_type="default")
        self.create_collection(collection_name="Quarterly_Averages", collection_type="default")
        self.create_collection(collection_name="USD_BOB_Tarjeta", collection_type="timeseries")

    def is_running(self):
        """
        Check if the MongoDB server is running and accessible.
        Prompts the user to retry if the connection fails.
        Raises:
            ConnectionError: If the connection cannot be established.
        """
        try:
            self.client.admin.command("ismaster")
        except ConnectionFailure:
            print("[main] Connection to MongoDB failed.")
            retry = input("[main] Do you want to retry? (y/n): ").lower()
            # If the user chooses to retry, check the MongoDB connection again.
            if retry == "y":
                try:
                    self.client.admin.command("ismaster")
                except ConnectionFailure:
                    # If the connection still fails, raise a ConnectionError and exit the program.
                    raise ConnectionError("[main] Connection to MongoDB failed. Exiting program.")
            else:
                # If the user chooses not to retry, raise a ConnectionError and exit the program.
                raise ConnectionError("[main] Exiting program.")

    def create_collection(self, collection_name, collection_type):
        """
        Create a MongoDB collection if it does not already exist.

        Args:
            collection_name (str): Name of the collection to create.
            collection_type (str): Type of the collection ("timeseries" or "default").
        """
        if collection_name not in self.db.list_collection_names():
            if collection_type == "timeseries":
                print(f"\n[mongo_controller] Creating timeseries collection {collection_name}...")
                self.db.create_collection(
                    collection_name,
                    timeseries={
                        "timeField": "timestamp",
                        "metaField": "metadata",
                        "granularity": "minutes",
                    },
                )
            elif collection_type == "default":
                print(f"\n[mongo_controller] Creating default collection {collection_name}...")
                self.db.create_collection(collection_name)
            print(f"[mongo_controller] Collection '{collection_name}' created successfully.")

    def save_data(self, collection, data):
        """
        Insert a single document into a specified collection.

        Args:
            collection (str): Name of the collection.
            data (dict): Document to insert.

        Returns:
            int: 0 if successful.
        """
        self.db[collection].insert_one(data)
        return 0

    def query_data(self, _mode, collection, _filter=None, projection=None, sort=None, limit=0, _datatype="df"):
        """
        Query data from a specified MongoDB collection.

        Args:
            _mode (str): The mode of the query. Can be "one" to fetch a single document or "all" to fetch multiple documents.
            collection (str): The name of the MongoDB collection to query.
            _filter (dict): The filter criteria for the query.
            projection (dict, optional): Fields to include or exclude.
            sort (tuple, optional): A tuple specifying the field and order to sort the results by. Defaults to None.
            limit (int, optional): The maximum number of documents to return. Defaults to 0 (no limit).
            _datatype (str, optional): The format of the returned data. Can be "df" for a pandas DataFrame or "cursor" for a MongoDB cursor. Defaults to "df".

        Returns:
            dict or pandas.DataFrame or pymongo.cursor.Cursor:
                If _mode is "one", returns a single document (dict).
                If _mode is "all" and _datatype is "df", returns a pandas DataFrame.
                If _mode is "all" and _datatype is "cursor", returns a MongoDB cursor.
        """
        if _filter is None:
            _filter = dict()
        if _mode == "one":
            return self.db[collection].find_one(_filter, projection)
        else:  # _mode == "all"
            result_cursor = self.db[collection].find(_filter, projection).sort("timestamp", sort).limit(limit)
            if _datatype == "df":
                return pd.DataFrame(list(result_cursor))
            else:  # _datatype == "cursor"
                return result_cursor

    def update_data(self, collection, _id, data):
        """
        Update a single document in a collection by its _id.

        Args:
            collection (str): Name of the collection.
            _id: The _id of the document to update.
            data (dict): Fields to update.
        """
        self.db[collection].update_one(
            {"_id": _id},
            {
                "$set": data
            }
        )

    def delete_data(self, collection, _id):
        """
        Delete a single document from a collection by its _id.

        Args:
            collection (str): Name of the collection.
            _id: The _id of the document to delete.
        """
        self.db[collection].delete_one({"_id": _id})

    def replace_data(self, collection, _id, data):
        """
        Replace a single document in a collection by its _id.

        Args:
            collection (str): Name of the collection.
            _id: The _id of the document to replace.
            data (dict): New document data.
        """
        self.db[collection].replace_one(
            {"_id": _id},
            data,
            upsert=True
        )


mongo_controller = MongoController()
