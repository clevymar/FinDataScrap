import datetime
import pandas as pd 
import json

import yfinance as yf
from tqdm import tqdm

import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

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
        res = yf.download(unds, start, end, ignore_tz=True,threads = isLocal(),progress=False)[field]
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
    resDB['Date']=resDB['Date'].dt.strftime('%Y-%m-%d')
    return resDB, res_clean

def TS_toDB(unds,table,field,verbose=True):
    resDB,res=download_clean_TS(unds,field=field,rounding=2)
    databases_update(resDB, table,idx=False,mode='replace',verbose=verbose, save_insqlite=True)
    return res


@timer
def import_yahoo(verbose=True):
    msgs=[]
    data = []
    for table,unds in dictInput.items():
        try:
            if table == 'EQTY_SPOTS':
                field = "Adj Close"
            else:
                field = "Close"
            if verbose: print(f'Processing {table} with {len(unds)} underlyings')
            res = TS_toDB(unds,table,field,verbose)
            msg = f'{table } well downloaded !!! - {len(res)} rows, {len(res.columns)} columns'
        except Exception as e:
            msg=f'Error while downloading {table}'
            res=None
            print(msg)
            print(e)
        finally:
            msgs.append(msg)
            data.append(res)
    if verbose:
        print('\n'.join(msgs))
    return data


def EQTYTS_last_date():
    return SQLA_last_date("EQTY_SPOTS")

ScrapYahoo = Scrap("YAHOO_SPOTS", import_yahoo, EQTYTS_last_date)

if __name__ == "__main__":
    print(import_yahoo(True))
    
