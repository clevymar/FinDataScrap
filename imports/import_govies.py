# %%
import os
import sys
from io import StringIO

import pandas as pd
from loguru import logger

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from common import last_bd
from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from scrap_selenium import start_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.webdriver import WebDriver

URL_ROOT = "http://www.worldgovernmentbonds.com/country/"
COUNTRIES = [
    "united-states",
    "germany",
    "france",
    "switzerland",
    "united-kingdom",
]

# %%


# %%


def maturity_string_to_nyears(s: str) -> float:
    tenorType = s.split()[1]
    tenor = int(s.split()[0])
    if tenorType.startswith("year"):
        res = tenor
    elif tenorType.startswith("month"):
        res = tenor / 12
    elif tenorType.startswith("week"):
        res = tenor / 52
    else:
        raise ValueError("Tenor unknown: " + s)
    return res


def get_curve(country: str, driver: WebDriver) -> pd.DataFrame:
    url = f"{URL_ROOT}{country}/"
    driver.get(url)
    try:
        _ = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "table-curve")))
        table_curve = driver.find_element(By.ID, "table-curve")
        tables = pd.read_html(StringIO(table_curve.get_attribute("outerHTML")))
        df = tables[0].iloc[:, [1, 2]]
        df.columns = ["Maturity", "Yield"]
        df.loc[:, "Yield"] = pd.to_numeric(df.loc[:, "Yield"].apply(lambda x: (x.replace("%", ""))), errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Could not access govie curve for {country} - reason {e}")
        return pd.DataFrame()


def scrap_govies(save_to_file: bool = True):
    dfAll = pd.DataFrame()
    try:
        driver = start_driver(headless=True)
        print(type(driver))
        for country in COUNTRIES:
            logger.info(f"Getting govies data for {country} at {URL_ROOT}{country}/")
            df = get_curve(country, driver)
            if len(df) > 0:
                df["country"] = country
                df["Date"] = last_bd
                if len(dfAll) == 0:
                    dfAll = df
                else:
                    dfAll = pd.concat([dfAll, df])
    finally:
        driver.quit()
    return dfAll


@timer
def govies_toDB(verbose=False):
    df = scrap_govies()
    df["nYears"] = df["Maturity"].apply(maturity_string_to_nyears)
    df = df[df["nYears"] <= 30]
    res = df[["Date", "nYears", "country", "Yield"]]
    res.columns = ["Date", "nYears", "Country", "Rate"]
    if verbose:
        print(res)
    databases_update(res, "GOVIES_TS", idx=False, mode="append", verbose=verbose, save_insqlite=True)
    return res


def import_govies(argument=None):
    msg = None
    try:
        res = govies_toDB()
        msg = f"Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols"
    except Exception as e:
        logger.exception(f"Error while downloading Govies: {e}")
        # raise Exception("Error while downloading Govies") from e
    return msg


def govies_last_date():
    return SQLA_last_date("GOVIES_TS")


ScrapGovies = Scrap("GOVIES", govies_toDB, govies_last_date)


if __name__ == "__main__":
    print(import_govies())
