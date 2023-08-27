"""
seems headless version, which sia  MUST for PA, does not work for CME

"""

import time
import pandas as pd
import traceback
from dataclasses import dataclass

from bs4 import BeautifulSoup

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


from utils import timer, print_color
from database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from common import last_bd, tod
from scrap_selenium import start_driver

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
    'ER':ProductDef("ER",None,'IRF'), #scrapped from Eurex
    'CH':ProductDef("CH",None,'IRF'), #scrapped from ICE

    # 'Gold':ProductDef("GC","metals/precious/gold",'Commo'),
    # 'Silver':ProductDef("SI","metals/precious/silver",'Commo'),
    # 'Oil':ProductDef("CL","energy/crude-oil/light-sweet-crude",'Commo'),
    # 'Gas':ProductDef("CL","energy/natural-gas/natural-gas",'Commo'),
    
    # 'Corn':ProductDef("ZC","agriculture/grains/corn",'Commo'),
    # 'Wheat':ProductDef("ZW","agriculture/grains/wheat",'Commo'),
    # 'Soybean':ProductDef("ZS","agriculture/oilseeds/soybean",'Commo'),
    # 'Cattle':ProductDef("LE","agriculture/livestock/live-cattle",'Commo'),
    # 'Hogs':ProductDef("HE","agriculture/livestock/lean-hogs",'Commo'),
    
    # 'Aluminium':ProductDef("ALI","metals/base/aluminum",'Commo'),
    # 'Copper':ProductDef("HG","metals/base/copper",'Commo'),
}

STEM = "https://www.cmegroup.com/markets/"
TAIL = ".settlements.html"
TAIL = ".quotes.html"
BTN_XPATH = "/html/body/main/div/div[3]/div[2]/div/div/div/div/div/div[2]/div/div/div/div/div/div[5]/div/div/div/div[2]/div[2]/button"

# BTN_XPATH = "/html/body/main/div/div[3]/div[3]/div/div/div/div/div/div[2]/div/div/div/div/div/div[6]/div/div/div/div[2]/div[2]/button"

# button_label="Load All"
# BTN_XPATH = f"//button[text()='{button_label}']"
# BTN_XPATH = f".//span[@class = 'text' and contains(text(), '{button_label}')]"



def _clean_price(s):
    try:
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
        python_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        print_color("Clicking on cookies button",'COMMENT')
        python_button.click()
    except:
        print('[-] Cant find cookies button')

    driver.execute_script("window.scrollTo(0, 1000);")
    time.sleep(3)

    for _ in range(2):
        try:
            python_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, BTN_XPATH)))
            print('button:', python_button)
            python_button.click()
            time.sleep(2)
            break
        except TimeoutException :
            print_color(f"[-] Timeout getting LOAD ALL button - might just not exist ! Scrolling down again instead",'FAIL')
            driver.execute_script("window.scrollTo(0, 1000);")
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
    isError=False
    messages=[]
    driver=start_driver()
    tab=[]
    try:
        for asset in dictAssets.keys():
            if asset in ['ER','CH']:
                pass
            else:
                try:   
                    if verbose: print(f'\nScrapping data for {asset} at {STEM + dictAssets[asset].coreURL + TAIL}')
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
                except Exception as e:
                    msg=f"Error scraping {asset}: {e}"  
                    print_color(msg,'FAIL')

    finally:
        print_color('Quitting Selenium driver','COMMENT')
        driver.quit()

    if len(tab)>0:
        df=pd.concat(tab)
        df['Date']=last_bd
        print(df)

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

if __name__ == "__main__":
    refresh_data()
