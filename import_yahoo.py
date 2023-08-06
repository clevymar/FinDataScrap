import datetime
import pandas as pd 

import yfinance as yf
from tqdm import tqdm

from common import start, end, EQUITY_UNDS
from utils import timer, isLocal
from database_sqlite import DB_update,DB_last_date
from database_mysql import SQL_update
from classes import Scrap


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
        res = yf.download(unds, start, end, ignore_tz=True,threads = isLocal())[field]
    except Exception as e:  
        # if pb usually coming from duplicated dates
        print('[-]Error while trying full download')
        print(e)
        print('Downloading data one by one')
        res = []
        for y in tqdm(unds):
            temp = yf.download(y, start, end,ignore_tz=True,progress=False)[field]
            temp = temp.rename(y)
            l1 = len(temp)
            temp = temp[~temp.index.duplicated()]
            l2 = len(temp)
            if l1 != l2:
                print(f"\t[-] Issue was with {y}: {l1-l2} dates duplicated")
            # res = res.join(temp, how="outer")
            res.append(temp)
        res=pd.concat(res,axis=1)
        print(res)
    res = res[~res.index.duplicated()]
    if rounding:
        res = res.round(rounding)
    resDB= res[pd.to_datetime(res.index).tz_localize(None) <= pd.to_datetime(end)]
    res_clean  = resDB.fillna(method="ffill")
    resDB=resDB.reset_index().rename(columns={resDB.index.name:'Date'})
    return resDB, res_clean

def TS_toDB(verbose=True):
    resDB,res=download_clean_TS(EQUITY_UNDS,rounding=2)
    DB_update(res, "EQTY_SPOTS",idx=False,mode='replace')
    SQL_update(res, "EQTY_SPOTS",idx=False,mode='replace',verbose=verbose)
    return res


@timer
def import_yahoo(verbose=True):
    try:
        res = TS_toDB(verbose)
        msg = f'Well downloaded !!! - {len(res)} rows, {len(res.columns)} columns'
        return msg
    except Exception as e:
        print('Error while downloading')
        print(e)
        return 'Error while downloading'

def EQTYTS_last_date():
    return DB_last_date("EQTY_SPOTS")

ScrapYahoo = Scrap("EQTY_SPOTS", TS_toDB, EQTYTS_last_date)


if __name__ == "__main__":
    print(import_yahoo(True))
    
