# %%
import os
import sys
from io import StringIO
import time

import pandas as pd
import requests
from icecream import ic
from rich.console import Console

console = Console()

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from common import last_bd
from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from scrap_selenium import start_driver, SeleniumError, WebDriverWait, EC, By,details_element

IRS_ccies = {"EUR": "Europe_Europe_EUR", "USD": "World_US_USD", "JPY": "World_JP_JPY", "CHF": "Europe_CH_CHF", "GBP": "Europe_GB_GBP"}

# %%
BTN_SHOW_MORE = (
    "/html/body/div[2]/main/div[2]/div/div[4]/section/div[2]/div/section/div/div/div[2]/div/div[1]/div/div[3]/div/div/div/div/div/div[2]/button"
)
ROOT_BTN_HEADER = "button-tab-navigation__tab-"

# //*[@id="2b5e32-button-tab-navigation__tab-0"]
# //*[@id="7cfab9-button-tab-navigation__tab-1"]
# //*[@id="871d63-button-tab-navigation__tab-2"]





url = "https://www.sparkasse.at/investments-en/markets/market-overview/money-market-fixed-income"

driver = start_driver(headless=False)
wait = WebDriverWait(driver, 5)
try:
    driver.get(url)
    # * deal with Cookies
    btnCookies = wait.until(EC.presence_of_element_located((By.ID, "popin_tc_privacy_button")))
    if btnCookies:
        console.log("Cookies button found")
        btnCookies.click()
        console.log("Cookies button clicked")
    else:
        console.log("Cookies button not found")

    # * deal with accept
    time.sleep(1)
    chkBoxAccept = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".css-95vv2o")))
    if chkBoxAccept:
        console.log("Checkbox found")
        chkBoxAccept.click()
        console.log("Checkbox clicked")
    else:
        console.log("Checkbox not found")

    btnAccept = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".btn--primary")))
    if btnAccept:
        console.log("Button Accept found")
        btnAccept.click()
        console.log("Button Accept clicked")
    else:
        console.log("Button Accept not found")

    html_source = driver.page_source
    time.sleep(1)

    btnBS = wait.until(EC.presence_of_element_located((By.XPATH, f"/html/body/div[2]/main/div[2]/div/nav/div[1]/div/div/ul/li[3]/a")))
    btnBS.click()

    section = driver.find_element(By.ID,'3')
    class_name = section.get_attribute('class')
    print(f'Section class: {class_name}')
    print(['\n\nButtons:'])
    buttons = section.find_elements(By.XPATH,".//button[@role='tab']")
    # Iterate over the buttons and print their text
    for button in buttons:
        console.log(f"Processing {button.text}")
        link = f"{ROOT_BTN_HEADER}{i}"
        btnHeader = wait.until(EC.presence_of_element_located((By.XPATH, f"//*[contains(@id, '{link}')]")))
        driver.execute_script("arguments[0].scrollIntoView();", btnHeader)
        time.sleep(1)
        section = driver.find_element(By.ID,'3')
        class_name = section.get_attribute('class')
        print(f'Section class: {class_name}')
        
        btnHeader = section.find_element(By.XPATH, f"//*[contains(@id, '{link}')]")
        details_element(btnHeader)
        time.sleep(1)
        if btnHeader:
            console.log(f"Button ccy {i} found")
            driver.execute_script("arguments[0].click();", btnHeader)
            # btnHeader.click() does not work, not sure why, hence using JS
            console.log(f"Button ccy {i} clicked")

            btnShowMore = wait.until(EC.element_to_be_clickable((By.XPATH, BTN_SHOW_MORE)))
            if btnShowMore:
                console.log(f"Button Show More found")
                driver.execute_script("arguments[0].click();", btnShowMore)
                console.log(f"Button Show More clicked")
                time.sleep(1)
            div = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "gem-comp-mio-api-table")))
            
            
            div = section.find_element(By.CLASS_NAME, "gem-comp-mio-api-table")
            tmp = div.get_attribute("innerHTML")
            tables = pd.read_html(StringIO(tmp))
            print(tables[0])
    
        time.sleep(1)
except Exception as e:
    raise Exception("Could not scrap the link at {link}") from e
finally:
    console.log("Quitting Selenium driver", "INFO")
    driver.quit()

# %%


#%%

