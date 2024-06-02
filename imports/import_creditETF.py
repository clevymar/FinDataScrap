# %%
from datetime import date
from pathlib import Path
import os
import sys
from dataclasses import dataclass
from typing import Optional

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)


import pandas as pd
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
from loguru import logger

from common import last_bd
from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap


# %%


@dataclass
class Asset:
    name: str
    url: str


@dataclass
class webField:
    name: str
    class_: str
    value: Optional[str] = None


defData = [
    webField("Index", "col-indexSeriesName"),  # indexSeriesName
    webField("YtM", "col-yieldToWorst"),
    webField("OAS", "col-optionAdjustedSpread"),  #col-optionAdjustedSpread
    webField("modDuration", "col-modelOad"),
]

listAssets = [
    Asset("BRLN", "https://www.ishares.com/us/products/329872/blackrock-floating-rate-loan-etf/"),
    Asset("HYG", "https://www.ishares.com/us/products/239565/ishares-iboxx-high-yield-corporate-bond-etf"),
    Asset("LQD", "https://www.ishares.com/us/products/239566/ishares-iboxx-investment-grade-corporate-bond-etf"),
    # Asset('IEAC','https://www.ishares.com/uk/individual/en/products/251726/ishares-core-corp-bond-ucits-etf')
]

# TODO add non US ETFs - probably need to use Selenium


def get_data(url):
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    res = []
    for field in defData:
        try:
            cl = field.class_
            value = soup.find("div", {"class": cl})
            value = value.find("div", {"class": "data"}).text.strip()
            field.value = value
            res.append(field)
        except:
            logger.error(f"{cl} field missing for {url}")
    return res


def retrieve_all_data():
    listResults = []
    for asset in listAssets:
        try:
            logger.info(f"Scrapping {asset.name} @ {asset.url}")
            name = asset.name
            res = get_data(asset.url)
            tmp = {"name": name}
            for field in res:
                tmp[field.name] = field.value
            listResults.append(tmp)
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred while processing {name}: {http_err}")
        except Exception as e:
            logger.error(f"Error processing {name}\n: {e}")

    return listResults


# %%


@timer
def creditETF_toDB(verbose=False):
    listResults = retrieve_all_data()
    dfRes = pd.DataFrame(listResults)
    dfRes["YtM"] = dfRes["YtM"].str.replace("%", "").astype(float).round(2)
    dfRes["OAS"] = (dfRes["OAS"].str.replace("bps", "").astype(float) / 100).round(2)
    dfRes["modDuration"] = dfRes["modDuration"].str.replace(" yrs", "").astype(float).round(2)
    dfRes["Date"] = last_bd
    if verbose:
        logger.info('Successful! Result:')
        print(dfRes)
    databases_update(dfRes, "CREDIT_ETF_TS", idx=False, mode="append", verbose=verbose, save_insqlite=True)
    return dfRes


def import_creditETF(verbose=False):
    msg = None
    try:
        res = creditETF_toDB(verbose=verbose)
        msg = f"Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols"
    except Exception as e:
        raise Exception("Error while downloading Credit ETF") from e
    return msg


def creditETF_last_date():
    return SQLA_last_date("CREDIT_ETF_TS")


ScrapCreditETF = Scrap("CREDIT ETF", creditETF_toDB, creditETF_last_date)


if __name__ == "__main__":
    print(import_creditETF(True))
