
#%%
import pandas as pd 
import time
import traceback
from io import StringIO

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from scrap_selenium import start_driver, _clean_price
from utils.utils import timer, print_color


urlCHF = "https://www.theice.com/products/72270612/Three-Month-Saron-Index-Futures-Contract/data?span=1"

urlEUR = 'https://www.eurex.com/ex-en/markets/int/mon/euribor-derivatives/euribor/Three-Month-EURIBOR-Futures-137458'
XPATH_CHEVRON="/html/body/div[5]/div[3]/div[1]/div[2]/div/div/div/div[2]/div[4]"
COOKIE_BUTTON = 'cookiescript_accept'


#%%
    
def get_webData(driver):
    driver.get(urlCHF)
    time.sleep(3)
    print_color("Looking for cookies button",'COMMENT')
    try:
        python_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        print_color("Clicking on cookies button",'COMMENT')
        python_button.click()
    except Exception as e:
        print_color('[-] Cant find cookies button','COMMENT')
    
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find( attrs={'class': 'table-bigdata'})
    bodies = table.find_all('tbody') 
    return bodies

def analyse_results(bodies):
    res=[]
    for body in bodies:
        for tr in body.find_all('tr'):
            els=tr.find_all('td')
            if len(els)==5:
                tab=[els[0].text,_clean_price(els[1].text)]
                res.append(tab)

    dfFuts=pd.DataFrame(res,columns=['Expiry','Last'])
    dfFuts['Settle']=dfFuts['Last']
    dfFuts['Expiry']=dfFuts['Expiry'].apply(lambda s:f"{s[1:4].upper()} 20{s[-2:]}")
    dfFuts=dfFuts.set_index("Expiry")
    return dfFuts




#%%


def euribor_futures(driver):
    driver.get(urlEUR)
    time.sleep(3)
    print_color("Looking for cookies button",'COMMENT')
    try:
        python_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS, "cookie-layover-button")))
        print_color("Clicking on cookies button",'COMMENT')
        python_button.click()
    except Exception as e:
        print_color('[-] Cant find cookies button','COMMENT')
    driver.execute_script("window.scrollBy(0, 1000)")
    driver.execute_script("window.scrollBy(0, 1000)")
    time.sleep(1)
    try:
        try:
            python_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, XPATH_CHEVRON)))
            try:
                for _ in range(5):
                    python_button.click()
            except Exception as e:
                raise e
        except TimeoutException :
            print(f"[-] Timeout getting button - might just not exist ! Scrolling down again instead")
            driver.execute_script("window.scrollBy(0, 1000)")
            time.sleep(0.5)
    except Exception as e:
        print(f"[-] Error processing: {e}")
        print(traceback.format_exc())
    html = driver.page_source
    dfs=pd.read_html(StringIO(html))
    df=dfs[0]
    df.rename(columns={'Date':'Expiry','Contract Date':'Expiry','D. Settle':'Settle'},inplace=True)
    dfSettlement=df[['Expiry','Last','Settle']]
    dfSettlement['Expiry']=pd.to_datetime(dfSettlement['Expiry'],dayfirst=True).dt.strftime('%b %Y').str.upper()
    dfSettlement.set_index('Expiry',inplace=True)
    return dfSettlement

def scrap_otherAsset(driver,asset:str='ER',verbose=False):
    if asset=='ER':
        dfFuts=euribor_futures(driver)
    else:
        bodies = get_webData(driver)
        dfFuts = analyse_results(bodies)
    if verbose:
        print(f"\n\n\***************************\n\{asset} -  futures curve")
        print(dfFuts)
    return dfFuts


if __name__ == "__main__":
    driver = start_driver(True,True)
    try:
        scrap_otherAsset(driver,'SARON',True)
    except Exception as e:
        raise e
    finally:
        print_color('Quitting Selenium driver','COMMENT')
        driver.quit()
