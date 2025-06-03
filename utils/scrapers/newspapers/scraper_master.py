from config import DBCONFIG
from utils.scrapers.newspapers.ahoradigital_scraper import ahoradigital_scraper
from utils.scrapers.newspapers.brujula_scraper import brujula_scraper
from utils.scrapers.newspapers.economy_scraper import economy_scraper
from utils.scrapers.newspapers.el_deber_scraper import el_deber_scraper
from utils.scrapers.newspapers.el_diario_scraper import el_diario_scraper
from utils.scrapers.newspapers.erbol_scraper import erbol_scraper
from utils.scrapers.newspapers.fides_scraper import fides_scraper
from utils.scrapers.newspapers.los_tiempos_scraper import los_tiempos_scraper
from utils.scrapers.newspapers.opinion_scraper import opinion_scraper
from utils.scrapers.newspapers.oxigeno_scraper import oxigeno_scraper
from utils.scrapers.newspapers.red_uno_scraper import red_uno_scraper


def scraper_master(source, timestamp_limit, debug):
    """
    Dispatches the scraping process to the appropriate newspaper scraper based on the source.

    Args:
        source (str): The name of the newspaper source to scrape.
        timestamp_limit (Any): The timestamp limit to filter articles (type depends on implementation).
        debug (bool): Flag to enable or disable debug mode.

    Returns:
        None

    Side Effects:
        Calls the corresponding scraper function for the given source.
        Updates the DBCONFIG if the scraper returns 0 (indicating an incomplete scrape).
    """
    result = 1
    match source:
        case "el_deber":
            result = el_deber_scraper(timestamp_limit, debug)
        case "el_diario":
            result = el_diario_scraper(timestamp_limit, debug)
        case "los_tiempos":
            result = los_tiempos_scraper(timestamp_limit, debug)
        case "red_uno":
            result = red_uno_scraper(timestamp_limit, debug)
        case "oxigeno":
            result = oxigeno_scraper(timestamp_limit, debug)
        case "erbol":
            result = erbol_scraper(timestamp_limit, debug)
        case "brujula":
            result = brujula_scraper(timestamp_limit, debug)
        case "opinion":
            result = opinion_scraper(timestamp_limit, debug)
        case "fides":
            result = fides_scraper(timestamp_limit, debug)
        case "economy":
            result = economy_scraper(timestamp_limit, debug)
        case "ahoradigital":
            result = ahoradigital_scraper(timestamp_limit, debug)
    if result == 0:
        DBCONFIG.update_config("newspaper_initial_complete_scrape", {source: False})
