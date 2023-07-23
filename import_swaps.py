import pandas as pd
import requests

from common import last_bd
from utils import timer
from database import DB_update,DB_last_date
from classes import Scrap

IRS_ccies={ "EUR":"Europe_Europe_EUR",
            "USD":"World_US_USD",
            "JPY":"World_JP_JPY",
            "CHF":"Europe_CH_CHF",
            "GBP":"Europe_GB_GBP"
            }


def get_oneIRS(ccy,verbose=True):
    urlroot="https://produkte.erstegroup.com/CorporateClients/en/MarketsAndTrends/Fixed_Income/Capital_markets_derivatives/index.phtml?elem999058_index=Table_SwapRates_"
    urlend="&elem999058_durationTimes=0"
    urlpart=IRS_ccies[ccy]
    url=urlroot+urlpart+urlend
    r = requests.get(url)
    r_html = r.text
    lst_df=pd.read_html(r_html)
    df=lst_df[1]
    df.columns=df.iloc[0]
    df.drop(df.index[0],inplace=True)
    tod=last_bd
    df["CCY"]=ccy
    df["Tenor"]=df["Name"].apply(lambda s: s[4:7].strip())
    df["Date"]=tod
    df["Rate"]=df["Current"]
    df=df[["Date","CCY","Tenor","Rate"]]
    if verbose:print(f"{ccy} IRS scrapped, {len(df)} tenors captured ")
    return df
        

def scrap_allIRS(verbose=True):
    res=[]
    for ccy in IRS_ccies:
        df=get_oneIRS(ccy)
        res.append(df)
    IRStable=pd.concat(res)
    IRStable['Rate']=IRStable['Rate'].astype(float)
    res= IRStable.copy()
    if verbose:
        IRStable['Tenor']=IRStable['Tenor'].apply(lambda s:'0'+s if len(s)==2 else s)
        pivot=pd.pivot_table(data=IRStable,values='Rate',index='Tenor',columns='CCY')
        print(pivot)
    return res

@timer
def IRS_toDB(verbose=True):
    res=scrap_allIRS(verbose)
    DB_update(res,"IRS_TS",idx=False,mode='append')
    return res

@timer
def import_swaps(verbose=True):
    try:
        res = IRS_toDB()
        msg = f'Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
        return msg
    except Exception as e:
        print('Error while downloading')
        print(e)
        return 'Error while downloading'


def swaps_last_date():
    return DB_last_date("IRS_TS")

ScrapIRS = Scrap("IRS", IRS_toDB, swaps_last_date)
