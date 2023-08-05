import datetime
import pandas as pd 
import yfinance as yf

from common import start, end, EQUITY_UNDS
from utils import timer


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

    res = yf.download(unds, start, end, ignore_tz=True,threads = True)[field]
    return res

    # try:
    #     res = yf.download(unds, start, end, ignore_tz=True,threads = True)[field]
    # except Exception as e:  
    #     # if pb usually coming from duplicated dates
    #     print('Error while trying full download')
    #     print(e)
    #     print('Downloading data one by one')
    #     raise e
    #     res = pd.DataFrame()
    #     for y in unds:
    #         temp = yf.download(y, start, end,ignore_tz=True,progress=False)[field]
    #         temp = temp.rename(y)
    #         l1 = len(temp)
    #         temp = temp[~temp.index.duplicated()]
    #         l2 = len(temp)
    #         if l1 != l2:
    #             print(f"\t[-] Issue was with {y}: {l1-l2} dates duplicated")
    #         res = res.join(temp, how="outer")
    # res = res[~res.index.duplicated()]
    # if rounding:
    #     res = res.round(rounding)
    # resDB= res[pd.to_datetime(res.index).tz_localize(None) <= pd.to_datetime(end)]
    # res_clean  = resDB.fillna(method="ffill")
    # return resDB, res_clean

@timer
def import_yahoo():
    try:
        resDB,res=download_clean_TS(EQUITY_UNDS,rounding=2)
        # print(res.head())
        # print(res.tail()) 
        msg = f'Well downloaded !!! - {len(res)} rows, {len(res.columns)} columns'
        return msg
    except Exception as e:
        print('Error while downloading')
        print(e)
        return 'Error while downloading'

if __name__ == "__main__":
    print(import_yahoo())