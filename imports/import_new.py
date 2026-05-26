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

from import_common import last_bd
from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from scrap_selenium import start_driver, SeleniumError, WebDriverWait, EC, By, details_element, WebElement

IRS_ccies = ["EUR", "USD", "JPY", "CHF", "GBP"]

# %%

verbose = True

SLEEP_TIME = 2
URL = "https://www.sparkasse.at/investments-en/markets/market-overview/money-market-fixed-income"
BTN_SHOW_MORE = (
    "/html/body/div[2]/main/div[2]/div/div[4]/section/div[2]/div/section/div/div/div[2]/div/div[1]/div/div[3]/div/div/div/div/div/div[2]/button"
)

# ROOT_BTN_HEADER = "button-tab-navigation__tab-"
# //*[@id="2b5e32-button-tab-navigation__tab-0"]
# //*[@id="7cfab9-button-tab-navigation__tab-1"]
# //*[@id="871d63-button-tab-navigation__tab-2"]


driver = start_driver(headless=False)
wait = WebDriverWait(driver, 5)


try:
    driver.get(URL)
    # * deal with Cookies
    try:
        go_through_cookies(verbose=verbose)
    except Exception as e:
        console.log(f"Could not deal with cookies: {e}")
        if verbose:
            console.print_exception()

    time.sleep(SLEEP_TIME)
    section = wait.until(EC.presence_of_element_located((By.ID, "3")))
    buttons = section.find_elements(By.XPATH, ".//button[@role='tab']")
    time.sleep(SLEEP_TIME)
    print("\n\nCurrency buttons found: " + ",".join([b.text for b in buttons]))
    res = []
    for button in buttons:
        try:
            section = scrap_ccy(section,button, verbose=verbose)
            df = create_df_from_section(section)
            res.append(df)
        except:
            console.log(f"Error while processing {button.text}")
            console.print_exception()

    IRStable = pd.concat(res)
    IRStable["Rate"] = IRStable["Rate"].astype(float)
    res = IRStable.copy()
    if verbose:
        IRStable["Tenor"] = IRStable["Tenor"].apply(lambda s: "0" + s if len(s) == 2 else s)
        pivot = pd.pivot_table(data=IRStable, values="Rate", index="Tenor", columns="CCY")
        print(pivot)
    # return res

except Exception as e:
    console.print_exception()
    raise Exception("Could not scrap the link at {link}") from e
finally:
    console.log("Quitting Selenium driver", "INFO")
    driver.quit()

# %%
