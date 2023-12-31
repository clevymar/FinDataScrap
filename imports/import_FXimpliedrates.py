import pandas as pd
import traceback

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from utils.utils import timer, print_color
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from common import last_bd
from scrap_selenium import start_driver


CCIES=[ {'ccy':"CHF",'inverse':False,'mult':100},
        {'ccy':"EUR",'inverse':True,'mult':100},
        {'ccy':"GBP",'inverse':True,'mult':100},
        {'ccy':"JPY",'inverse':False,'mult':1},
        {'ccy':"NOK",'inverse':False,'mult':100},
        {'ccy':"TRY",'inverse':False,'mult':100},
        ]


def year_frac(offset:str):
    t=offset[-1].upper()
    num=offset[:-1]
    pb=False
    if t=="W":
        mult=1/52
    elif t=="M":
        mult=1/12
    elif t=="Y":
        mult=1
    else:
        pb=True

    if pb:
        res=None
        print(f"Problem with {offset}")
    else:
        res=float(num)*mult
    return res

def _implied_rate_oneccy(driver,ccy:str,inverse:bool=False,mult=100,verbose=True)-> pd.DataFrame:
    if inverse:
        link="https://www.investing.com/currencies/"+ ccy.lower() + "-usd-forward-rates"
    else:
        link="https://www.investing.com/currencies/usd-" + ccy.lower() + "-forward-rates"
    if verbose: print(f"\n Computing implied rates for {ccy} at {link}")

    html_source = driver.get(link)
    
    spotElement = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, 'last_last')))
    tableElement = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, 'curr_table')))
    
    spot=float(spotElement.text)
    if verbose:
        print(f" \n Spot for {ccy} is = {spot}")

    tables = pd.read_html(tableElement.get_attribute('outerHTML'))
    df=tables[0]
    dfres=df.loc[:,["Name","Bid","Ask","Time"]]
    """drop the first lines """
    dfres["Name"]=dfres["Name"].apply(lambda s:s[7:10].strip())
    dfres=dfres[~dfres.Name.isin(['ON','TN','SN','SW'])]
    """ now put in usable form """
    dfres["Bid"]=dfres["Bid"].astype('float')/spot/mult
    dfres["Ask"]=dfres["Ask"].astype('float')/spot/mult
    dfres["YearFrac"]=dfres["Name"].apply(lambda s:year_frac(s)).round(3)
    dfres["Implied rate spread"]=((dfres["Bid"]+dfres["Ask"])/2/dfres["YearFrac"]).round(2)
    if inverse:
       dfres["Implied rate spread"]=dfres["Implied rate spread"]*-1     
    dfres.set_index("Name",inplace=True)
    return dfres



def implied_rates(verbose=True)-> pd.DataFrame:
    """
    The function `implied_rates` computes implied rates for different currencies and returns a DataFrame
    with the results.
    
    """
    driver = start_driver()
    try:
        tab=[]
        for row in CCIES:
            ccy=row['ccy']
            try:
                dfccy=_implied_rate_oneccy(driver,ccy,row['inverse'],row['mult'],verbose=verbose)
                dfccy["Currency"]=ccy
                tab.append(dfccy)
            except Exception as e:
                print_color(f"[-] Error computing implied rates for {ccy}",'FAIL')
                print(e)
                print(traceback.format_exc())
    except Exception as e:
        raise Exception("Could not scrap the implied rates for {ccy}") from e
    finally:
        print_color('Quitting Selenium driver','COMMENT')
        driver.quit()
        
    df=pd.concat(tab)     
    df.reset_index(inplace=True)
    df.rename(columns={"Name":"Tenor"},inplace=True)
    df["Name"]=df["Currency"]+" "+df["Tenor"]
    df['Date']=last_bd
    df.set_index("Name",inplace=True)
    df=df[["Currency","Tenor","Implied rate spread","Bid","Ask","YearFrac",'Date']]
    return df


@timer
def saveFXImpliedRates_toDB(verbose=False):
    df = implied_rates(verbose=verbose)
    if verbose:
        print_color(f'*** FX Implied rates ***',color='RESULT')
        print(df)
    databases_update(df,"FX_IMPLIED_RATES",idx=False,mode='append',verbose=verbose, save_insqlite=True)
    return df


def import_FXImpliedRates(verbose=True)->str:
    msg=None
    try:
        res = saveFXImpliedRates_toDB(verbose=verbose)
        msg = f'FX Implied Rates well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
    except Exception as e:
        msg='Error while downloading FX Implied Rates'
        print_color(msg,'FAIL')
        print(e)
   
    return msg

def FXImpliedRates_last_date():
    return SQLA_last_date("FX_IMPLIED_RATES")




ScrapFXImpliedRates = Scrap("FX IMPLIED RATES", saveFXImpliedRates_toDB, FXImpliedRates_last_date)


if __name__ == "__main__":
    saveFXImpliedRates_toDB(True)
    