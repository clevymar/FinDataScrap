"""
seems headless version, which sia  MUST for PA, does not work for CME

"""
import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

import time
import pandas as pd
import traceback
from dataclasses import dataclass

from bs4 import BeautifulSoup

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException


from utils.utils import timer, print_color
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from common import last_bd, tod
from scrap_selenium import start_driver

#TODO add Eurex and CHF
# import eurex_curves
# import CHF_curve

#* COPIED from CME_common - not great
@dataclass
class ProductDef:
    """Class defining an asset"""
    ticker: str
    coreURL: str
    assetType: str 
    
dictAssets={
    'SOFR':ProductDef("SR","interest-rates/stirs/three-month-sofr",'IRF'), #SOFR futures
    'FF':ProductDef("FF","interest-rates/stirs/30-day-federal-fund",'IRF'),

    # 'ER':ProductDef("ER",None,'IRF'), #scrapped from Eurex
    # 'CH':ProductDef("CH",None,'IRF'), #scrapped from ICE

    'Gold':ProductDef("GC","metals/precious/gold",'Commo'),
    'Silver':ProductDef("SI","metals/precious/silver",'Commo'),
    'Oil':ProductDef("CL","energy/crude-oil/light-sweet-crude",'Commo'),
    'Gas':ProductDef("CL","energy/natural-gas/natural-gas",'Commo'),
    
    'Corn':ProductDef("ZC","agriculture/grains/corn",'Commo'),
    'Wheat':ProductDef("ZW","agriculture/grains/wheat",'Commo'),
    'Soybean':ProductDef("ZS","agriculture/oilseeds/soybean",'Commo'),
    'Cattle':ProductDef("LE","agriculture/livestock/live-cattle",'Commo'),
    'Hogs':ProductDef("HE","agriculture/livestock/lean-hogs",'Commo'),
    'Sugar':ProductDef("YO","agriculture/lumber-and-softs/sugar-no11",'Commo'),
    
    'Aluminium':ProductDef("ALI","metals/base/aluminum",'Commo'),
    'Copper':ProductDef("HG","metals/base/copper",'Commo'),
}

#TODO  1) Zinc missing 2) replace CME for Sugar, WHeat...by correct exchange ?


STEM = "https://www.cmegroup.com/markets/"
TAIL = ".settlements.html"
TAIL = ".quotes.html"
BTN_XPATH = "/html/body/main/div/div[3]/div[2]/div/div/div/div/div/div[2]/div/div/div/div/div/div[5]/div/div/div/div[2]/div[2]/button"
BTN_XPATH = "/html/body/main/div/div[3]/div[3]/div/div/div/div/div/div[2]/div/div/div/div/div/div[6]/div/div/div/div[2]/div[2]/button"




def _clean_price(s):
    try:
        if s == "-":
            return 0
        s = s.replace("'", ".")
        return float(s[:5])
    except Exception as e:
        print(e)
        return None

#%%


def _get_webData(driver,coreURL: str):
    url = STEM + coreURL + TAIL
    driver.get(url)
    time.sleep(2)
   
    try:
        # driver.execute_script("window.scrollTo(0, 1000)")
        python_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        print_color("Clicking on cookies button",'COMMENT')
        python_button.click()
    except:
        print('[-] Cant find cookies button')

    driver.execute_script("window.scrollTo(0, 1000);")
    time.sleep(1)

    for _ in range(2):
        try:
            python_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, BTN_XPATH)))
            # driver.execute_script("arguments[0].scrollIntoView({ behavior: 'auto', block: 'center', inline: 'center' });", button)
            # print('button:', python_button)
            python_button.click()
            time.sleep(2)
            break
        except TimeoutException :
            print_color(f"[-] Timeout getting LOAD ALL button - might just not exist ! Scrolling down again instead",'FAIL')
            driver.execute_script("window.scrollBy(0, 1000);")
            # driver.execute_script('window.scrollBy(0, window.innerHeight);')
            time.sleep(1)
        except ElementClickInterceptedException:
            print_color(f"[-] Could not click the button - might just not exist ! Scrolling down again instead",'FAIL')
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(1)
        except Exception as e:
            print_color(f"[-] Error processing: {e}",'FAIL')
            print(traceback.format_exc())
    
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find(attrs={"class": "main-table-wrapper"})
    table_body = table.find("tbody")
    return table_body






def _process_results(table_body):
    res = []
    for tr in table_body.find_all("tr"):
        els = tr.find_all("td")
        tab = [
            els[0].text[:8],
            _clean_price(els[3].text),
            _clean_price(els[5].text),
        ]
        res.append(tab)

    dfFuts = pd.DataFrame(res, columns=["Expiry", "Last", "Settle"])
    dfFuts = dfFuts[dfFuts["Settle"]>0]
    dfFuts = dfFuts.set_index("Expiry")
    return dfFuts



def scrap_asset(driver,asset:str,verbose=True):
    dfFuts=None
    assetDef = dictAssets[asset]
    coreURL = assetDef.coreURL
    if len(coreURL)>0:
        try:
            table_body = _get_webData(driver,coreURL)
        except:
            print_color(f"[-] Error getting data for {asset}",'FAIL')
            print(traceback.format_exc())
            return None
        try:
            dfFuts = _process_results(table_body)
        except:
            print_color(f"[-] Error processing data for {asset}",'FAIL')
            print(traceback.format_exc())
            return None
        if verbose:
            print_color(f"\n\n\***************************\n{asset} - {assetDef.ticker} futures curve",'RESULT')
            print(dfFuts)
        return dfFuts
    else:
        return None

def compose_html_msg(messages):
    msg=f"<h1>Scraping futures curves</h1><br>"
    for m in messages:
        msg+=f"<p>{m}</p><br>"  #-------------------<br>
    return msg


def refresh_data(verbose=True):
    driver=None
    driver=start_driver(headless=True)
    tab=[]
    try:
        for asset in dictAssets.keys():
            if asset in ['ER','CH']:
                pass
            else:
                try:   
                    if verbose: print_color(f'\nScrapping data for {asset} at {STEM + dictAssets[asset].coreURL + TAIL}','COMMENT')
                    df=scrap_asset(driver,asset,verbose=False)
                    if len(df)>0: 
                        df['asset']=asset
                        tab.append(df)
                    if verbose:
                        if len(df)>0: 
                            msg=f"Scraped {asset} - {len(df)} maturities returned"
                            print_color(msg,'RESULT')
                        else:
                            msg=f"No data returned for {asset}"
                            print_color(msg,'FAIL')
                except KeyboardInterrupt:
                    print_color('Quitting Selenium driver','COMMENT')
                    driver.quit()
                    print_color('Exiting...','COMMENT')
                    exit(0)
                except Exception as e:
                    msg=f"Error scraping {asset}: {e}"  
                    print_color(msg,'FAIL')
    
    except KeyboardInterrupt:
        if driver:
            print_color('Quitting Selenium driver','COMMENT')
            driver.quit()
        print_color('Exiting...','COMMENT')
        exit(0)

    finally:
        print_color('Quitting Selenium driver','COMMENT')
        if driver: driver.quit()

    if len(tab)>0:
        df=pd.concat(tab)
        df['Date']=last_bd
        return df

    # asset='ER'
    # try:
    #     dfFuts=eurex_curves.scrap_asset(asset)
    #     save_data(dfFuts, asset)
    #     l=len(dfFuts)
    #     msg=f"Scraped {asset} - {l} maturities returned"
    #     LOGGER.info(msg)
    # except Exception as e:
    #     msg=f"Error scraping {asset}: {e}"
    #     LOGGER.error(msg)
    #     isError=True
    # messages.append(msg)

    # asset='CH'
    # try:
    #     dfFuts=CHF_curve.scrap_curve()
    #     save_data(dfFuts, asset)
    #     l=len(dfFuts)
    #     msg=f"Scraped {asset} - {l} maturities returned"
    #     LOGGER.info(msg)
    # except Exception as e:
    #     msg=f"Error scraping {asset}: {e}"
    #     LOGGER.error(msg)
    #     isError=True
    # messages.append(msg)
    
    # #*send email with the messages
    
    # if isError:
    #     title = "Attention ERROR - Futures curves scrapping"
    # else:
    #     title = "Futures curves scrapping"
    # send_email(title,compose_html_msg(messages))


def TS_toDB(data,table,verbose=True):
    resDB = refresh_data(verbose=verbose)
    databases_update(resDB, table,idx=False,mode='replace',verbose=verbose, save_insqlite=True)
    return res


def import_futs_curves(verbose=False):
    resDB = refresh_data(verbose=verbose)
    scrappedUnds= resDB['asset'].unique().tolist()
    missingUnds = [c for c in dictAssets.keys() if c not in scrappedUnds]
    if len(missingUnds)>0:
        print_color(f"[-] No data was scrapped for {missingUnds}",'FAIL')
    print_color(f'[+]{len(scrappedUnds)} future curves scrapped,  {len(resDB)} lines saved in DB',"RESULT")
    databases_update(resDB.reset_index(), "FUTURES_CURVES",idx=False,mode='append',verbose=verbose, save_insqlite=True)


def CMEFUTS_last_date():
    return SQLA_last_date("FUTURES_CURVES")

ScrapYahoo = Scrap("FUTURES_CURVES", import_futs_curves, CMEFUTS_last_date)


if __name__ == "__main__":
    refresh_data(True)
    exit(0)
    import_futs_curves(True)
