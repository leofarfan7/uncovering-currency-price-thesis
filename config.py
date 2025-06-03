import pyprojroot
from pymongo import MongoClient

# Base directory
BASE_DIR = pyprojroot.here()  # This is the root of the project, detected automatically

# Directories
UTILS_DIR = BASE_DIR / "utils"
DATA_DIR = BASE_DIR / "data"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
CMV_DIR = DATA_DIR / "cmv"
GRAPHS_DIR = DATA_DIR / "graphs"
LIQUIDITY_DEPTH_DIR = GRAPHS_DIR / "liquidity_depth"
TWENTY_FOUR_HOURS_PRICE_DIR = GRAPHS_DIR / "twenty_four_hours_price"
ONE_WEEK_PRICE_DIR = GRAPHS_DIR / "one_week_price"
TWO_WEEKS_PRICE_DIR = GRAPHS_DIR / "two_weeks_price"
BI_HOUR_PRICE_DIR = GRAPHS_DIR / "bi_hour_price"
ALL_TIME_PRICE_DIR = GRAPHS_DIR / "all_time_price"

# MongoDB Settings
MONGO_HOST = "localhost"
MONGO_PORT = 27017

# Newspaper Scraping Settings
USER_AGENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
DOLAR_HOY_URL = "https://dolarhoy.com/historico-dolar-blue"
EL_DEBER_URL = "https://eldeber.com.bo"
LOS_TIEMPOS_URL = "https://www.lostiempos.com"
EL_DIARIO_URL = "https://www.eldiario.net"
RED_UNO_URL = "https://www.reduno.com.bo"
OXIGENO_URL = "https://oxigeno.bo"
ERBOL_URL = "https://www.erbol.com.bo"
BRUJULA_URL = "https://brujuladigital.net"
OPINION_URL = "https://www.opinion.com.bo"
FIDES_URL = "https://www.noticiasfides.com"
ECONOMY_URL = "https://www.economy.com.bo"
AHORADIGITAL_URL = "https://www.ahoradigital.net"

# General Variables
RECORD_INTERVAL = 5  # Interval in minutes to record data


# Class to interact with the config collection in the database
class DBConfig:
    def __init__(self):
        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        db = client.bolivian_blue_db
        self.collection = db.config

    def get_config(self, setting):
        """
        Get the configuration data for a specific setting from the config collection.

        Args:
            setting (str): The name of the setting to retrieve.

        Returns:
            dict: The configuration data for the specified setting.
        """
        data = self.collection.find_one({"setting": setting})
        if data is not None:
            return data
        else:
            self.collection.insert_one({"setting": setting})
            return self.get_config(setting)

    def update_config(self, setting, data):
        """
        Update the configuration data for a specific setting in the config collection.

        Args:
            setting (str): The name of the setting to update.
            data (dict): The new configuration data for the specified setting.

        Returns:
            int: 0 if the data is updated successfully.
        """
        self.collection.update_one(
            {"setting": setting},
            {"$set": data},
            upsert=True
        )
        return 0


# Initialize the DBConfig class
DBCONFIG = DBConfig()

# TradingView Settings
TRADINGVIEW_USERNAME = DBCONFIG.get_config("TRADINGVIEW_CREDENTIALS")["USERNAME"]
TRADINGVIEW_PASSWORD = DBCONFIG.get_config("TRADINGVIEW_CREDENTIALS")["PASSWORD"]

# LLM Settings
LOCAL_API_URL = "http://localhost:11434/v1"
LOCAL_API_KEY = "ollama"
AI_MODE = "local"  # Either 'local', 'groq', or 'huggingface'
