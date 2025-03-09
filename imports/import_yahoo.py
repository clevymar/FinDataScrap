import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)
import datetime
import json

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from loguru import logger


from common import start, end, DIR_FILES, fichierTSUnderlyings
from utils.utils import timer, isLocal
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap

dictInput = json.load(open(fichierTSUnderlyings, "r"))


def download_clean_TS(unds: list, field: str = "Adj Close", rounding: int = None):
    """
    Download and clean time series data for a list of tickers.

    Args:
        unds (list): List of tickers to download data for.
        field (str, optional): Field to download. Defaults to "Adj Close".
        rounding (int, optional): Number of decimal places to round to. Defaults to None.

    Returns:
        tuple: Tuple containing two pandas DataFrames. The first is the raw data with no cleaning, and the second is the cleaned data.
    """

    try:
        logger.info(f'Starting download process for {len(unds)} underlyings')
        res = yf.download(unds, start, end, ignore_tz=True, threads=isLocal(), progress=False)[field]
    except Exception as e:
        # if pb usually coming from duplicated dates
        logger.error(f"[-]Error while trying full download:\n{e}")
        logger.info("Downloading data one by one")
        res = []
        for y in tqdm(unds):
            temp = yf.download(y, start, end, ignore_tz=True, progress=False)[field]
            temp = temp.rename(y)
            l1 = len(temp)
            temp = temp[~temp.index.duplicated()]
            l2 = len(temp)
            if l1 != l2:
                logger.info(f"\t[-] Issue was with {y}: {l1-l2} dates duplicated")
            # res = res.join(temp, how="outer")
            res.append(temp)
        res = pd.concat(res, axis=1)
    if len(res)>0:
        res = res[~res.index.duplicated()]
        if rounding:
            res = res.round(rounding)
        resDB = res[pd.to_datetime(res.index).tz_localize(None) <= pd.to_datetime(end)]
        res_clean = resDB.ffill()
        resDB = resDB.reset_index().rename(columns={resDB.index.name: "Date"})
        resDB["Date"] = resDB["Date"].dt.strftime("%Y-%m-%d")
        logger.success(f"{len(resDB)} rows downloaded for {len(resDB.columns)} underlyings")
    else:
        resDB = pd.DataFrame()
        res_clean = pd.DataFrame()
        logger.error("No data downloaded")
    return resDB, res_clean


def TS_toDB(unds, table, field, verbose=True):
    resDB, res = download_clean_TS(unds, field=field, rounding=2)
    if len(resDB)>0:
        databases_update(resDB, table, idx=False, mode="replace", verbose=verbose, save_insqlite=True)
    return res


@timer
def import_yahoo(verbose=True):
    msgs = []
    data = []
    for table, unds in dictInput.items():
        try:
            if table == "EQTY_SPOTS":
                field = "Adj Close"
            else:
                field = "Close"
            if verbose:
                logger.info(f"Processing {table} with {len(unds)} underlyings")
            res = TS_toDB(unds, table, field, verbose)
            msg = f"{table } well downloaded !!! - {len(res)} rows, {len(res.columns)} columns"
        except Exception as e:
            msg = f"Error while downloading {table}"
            res = None
            logger.error(f"{msg}\n\t{e}")
        finally:
            msgs.append(msg)
            data.append(res)
    if verbose:
        logger.info("\n".join(msgs))
    return data


def EQTYTS_last_date():
    return SQLA_last_date("EQTY_SPOTS")


ScrapYahoo = Scrap("YAHOO_SPOTS", import_yahoo, EQTYTS_last_date)

if __name__ == "__main__":
    print(import_yahoo(True))
