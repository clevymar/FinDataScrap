#%%
import datetime
import pandas as pd 
import json

import yfinance as yf
from icecream import ic
# from tqdm import tqdm

LIST_FIELDS = ['yield','trailingAnnualDividendYield']
#%%

sec='EEM'

security = yf.Ticker(sec)
# hist = security.history(period="1mo")
# hist

print(security.fast_info)
# print(security.dividends)
# print(security.info['dividendRate'])
# for key, value in security.info.items():
#     print(key,':\t', value)

info = security.info

for key in LIST_FIELDS:
    if key in info:
        print(key,':\t', info[key])


#%%



def div_yield(security:str):
    ticker = yf.Ticker(security)
    info = ticker.info
    dictReturn = {}
    for field in LIST_FIELDS:
        if field in info:
            dictReturn[field] = info[field]
    
    return dictReturn
            

for sec in ["MSFT",'AAPL','JPM','PICK',"EEM",'PXH']:
    print(sec) 
    res = div_yield(sec)
    print(res)
    

#%%

div_yield('EEM')

#%%
from loguru import logger
import sys

fmt = "{time:DD/MM HH:mm:ss} | {level} | {function}:{line} - {message}"

logger.remove()
logger.add(sys.stderr, format=fmt, level="INFO")

def test_func():
    logger.info("Test message")
    
test_func()

