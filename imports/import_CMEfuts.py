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
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from loguru import logger

from utils.utils import timer, print_color
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from common import last_bd, tod
from scrap_selenium import start_driver, _clean_price
from import_otherfuts import scrap_otherAsset

# TODO add Eurex and CHF
# import eurex_curves
# import CHF_curve


fmt = "<green>{time:DD/MM HH:mm:ss}</green> | <level>{level}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
logger.remove()
logger.add(sys.stderr, format=fmt, level="DEBUG",backtrace=True, diagnose=False)
logger.add("Files/import_CMEfuts.log",  format=fmt,level="DEBUG", rotation="1 week", backtrace=True, diagnose=False)


# * COPIED from CME_common - not great
@dataclass
class ProductDef:
    """Class defining an asset"""

    ticker: str
    coreURL: str | None
    assetType: str


dictAssets = {
    "SOFR": ProductDef("SR", "interest-rates/stirs/three-month-sofr", "IRF"),  # SOFR futures
    "FF": ProductDef("FF", "interest-rates/stirs/30-day-federal-fund", "IRF"),
    "Gold": ProductDef("GC", "metals/precious/gold", "Commo"),
    "Silver": ProductDef("SI", "metals/precious/silver", "Commo"),
    "Oil": ProductDef("CL", "energy/crude-oil/light-sweet-crude", "Commo"),
    "Gas": ProductDef("CL", "energy/natural-gas/natural-gas", "Commo"),
    "Aluminium": ProductDef("ALI", "metals/base/aluminum", "Commo"),
    "Copper": ProductDef("HG", "metals/base/copper", "Commo"),
    "Corn": ProductDef("ZC", "agriculture/grains/corn", "Commo"),
    "Wheat": ProductDef("ZW", "agriculture/grains/wheat", "Commo"),
    "Soybean": ProductDef("ZS", "agriculture/oilseeds/soybean", "Commo"),
    "Cattle": ProductDef("LE", "agriculture/livestock/live-cattle", "Commo"),
    "Hogs": ProductDef("HE", "agriculture/livestock/lean-hogs", "Commo"),
    # "Sugar": ProductDef("YO", "agriculture/lumber-and-softs/sugar-no11", "Commo"),
    "ER": ProductDef("ER", None, "IRF"),  # scrapped from Eurex
    "CH": ProductDef("CH", None, "IRF"),  # scrapped from ICE
    "HSCEI": ProductDef("HHI", None, "Equity"),  # scrapped from HKEX
}

# TODO  1) Zinc missing 2) replace CME for Sugar, WHeat...by correct exchange ?


STEM = "https://www.cmegroup.com/markets/"
TAIL = ".settlements.html"
TAIL = ".quotes.html"
BTN_XPATH = "/html/body/main/div/div[3]/div[2]/div/div/div/div/div/div[2]/div/div/div/div/div/div[5]/div/div/div/div[2]/div[2]/button"
BTN_XPATH = "/html/body/main/div/div[3]/div[3]/div/div/div/div/div/div[2]/div/div/div/div/div/div[6]/div/div/div/div[2]/div[2]/button"

BTN_COOKIES = '//*[@id="onetrust-accept-btn-handler"]'


# %%


def _get_webData(driver, coreURL: str, clickCookies: bool = True):
    def _scroll_down(forCME: bool = True):
        if forCME:
            driver.find_element(By.XPATH, "//body").send_keys(Keys.PAGE_DOWN)
        else:
            driver.execute_script("window.scrollTo(0, 1000)")

    url = STEM + coreURL + TAIL
    driver.get(url)
    time.sleep(2)
    hasClickedCookies = False

    if clickCookies:
        logger.info("Looking for cookies button")
        try:
            _scroll_down()
            python_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            logger.info("Clicking on cookies button")
            python_button.click()
            hasClickedCookies = True
        except Exception as e:
            logger.warning("[-] Cant find cookies button")

    _scroll_down()
    time.sleep(1)

    for _ in range(2):
        try:
            python_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, BTN_XPATH)))
            python_button.click()
            time.sleep(2)
            break
        except TimeoutException:
            logger.warning("[-] Timeout getting LOAD ALL button - might just not exist ! Scrolling down again instead")
            _scroll_down()
            time.sleep(1)
        except ElementClickInterceptedException:
            logger.warning("[-] Could not click the button - might just not exist ! Scrolling down again instead")
            _scroll_down()
            time.sleep(1)
        except Exception as e:
            logger.exception(f"[-] Error processing: {e}")
            # print(traceback.format_exc())

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find(attrs={"class": "main-table-wrapper"})
    table_body = table.find("tbody")
    return table_body, hasClickedCookies


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
    dfFuts = dfFuts[dfFuts["Settle"] > 0]
    dfFuts = dfFuts.set_index("Expiry")
    return dfFuts


def scrap_asset(driver, asset: str, verbose=True, need_to_click_cookies=True):
    dfFuts = None
    assetDef = dictAssets[asset]
    coreURL = assetDef.coreURL
    if coreURL:
        try:
            table_body, hasClickedCookies = _get_webData(driver, coreURL, need_to_click_cookies)
        except:
            logger.exception(f"[-] Error getting data for {asset}")
            # print(traceback.format_exc())
            return None
        try:
            dfFuts = _process_results(table_body)
        except:
            logger.exception(f"[-] Error processing data for {asset}", "FAIL")
            # print(traceback.format_exc())
            return None
        if verbose:
            # print_color(f"\n\n\***************************\n{asset} - {assetDef.ticker} futures curve", "RESULT")
            logger.success(f"{asset} - {assetDef.ticker} future curve scrapped")
            print(dfFuts)
        else:
            logger.success(f"*** {asset} - {assetDef.ticker} future curve scrapped - {len(dfFuts)} rows***")
        return dfFuts, hasClickedCookies
    else:
        return None, False


def compose_html_msg(messages):
    msg = "<h1>Scraping futures curves</h1><br>"
    for m in messages:
        msg += f"<p>{m}</p><br>"  # -------------------<br>
    return msg


def refresh_data(verbose=True) -> pd.DataFrame:
    driver = None
    driver = start_driver(headless=True, forCME=True)
    tab = []
    need_to_click_cookies = True
    counter = 0
    totalAssets = len(dictAssets)
    try:
        for asset, product in dictAssets.items():
            counter += 1
            if asset in ["ER", "CH", "HSCEI"]:
                try:
                    if verbose:
                        logger.info(f"\nScrapping data for {asset} [{counter}/{totalAssets}] ")
                    df = scrap_otherAsset(driver, asset, verbose=False)
                    if len(df) > 0:
                        df["asset"] = asset
                        tab.append(df)
                    if verbose:
                        if len(df) > 0:
                            msg = f"Scraped {asset} - {len(df)} maturities returned"
                            logger.success(msg)
                        else:
                            msg = f"No data returned for {asset}"
                            logger.warning(msg)
                except KeyboardInterrupt:
                    logger.info("Quitting Selenium driver")
                    driver.quit()
                    logger.info("Exiting...")
                    exit(0)
                except Exception as e:
                    msg = f"Error scraping {asset}: {e}"
                    logger.exception(msg)
            else:
                try:
                    if verbose:
                        logger.info(f"\nScrapping data for {asset} [{counter}/{totalAssets}] \n\tat {STEM}{product.coreURL}{TAIL}")
                    df, hasClickedCookies = scrap_asset(driver, asset, verbose=False, need_to_click_cookies=need_to_click_cookies)
                    if df is None:
                        df = pd.DataFrame()
                    if len(df) > 0:
                        df["asset"] = asset
                        tab.append(df)
                        if need_to_click_cookies:
                            need_to_click_cookies = not hasClickedCookies
                        else:
                            need_to_click_cookies = False
                    if verbose:
                        if len(df) > 0:
                            msg = f"Scraped {asset} - {len(df)} maturities returned"
                            logger.success(msg)
                        else:
                            msg = f"No data returned for {asset}"
                            logger.warning(msg)
                except KeyboardInterrupt:
                    logger.info("Quitting Selenium driver")
                    driver.quit()
                    logger.info("Exiting...")
                    exit(0)
                except Exception as e:
                    msg = f"Error scraping {asset}: {e}"
                    logger.exception(msg)

    except KeyboardInterrupt:
        if driver:
            logger.info("Quitting Selenium driver")
            driver.quit()
        logger.info("Exiting...")
        exit(0)

    finally:
        logger.info("Quitting Selenium driver")
        if driver:
            driver.quit()

    if len(tab) > 0:
        df = pd.concat(tab)
        df["Date"] = last_bd
        return df
    return pd.DataFrame()

@logger.catch
def import_futs_curves(verbose=False) -> tuple[str, str]:
    tries = 0
    MAX_TRIES = 2
    while tries <= MAX_TRIES:
        tries += 1
        resDB = refresh_data(verbose=verbose)
        scrappedUnds = resDB["asset"].unique().tolist()
        missingUnds = [c for c in dictAssets.keys() if c not in scrappedUnds]
        if len(missingUnds) > 0:
            logger.error(f"[-] No data was scrapped for {missingUnds}")
            if tries <= MAX_TRIES and len(missingUnds) > 3:
                logger.info("Trying again...")
                time.sleep(10)
            else:
                tries = MAX_TRIES + 1
        else:
            tries = MAX_TRIES + 1

    databases_update(resDB.reset_index(), "FUTURES_CURVES", idx=False, mode="append", verbose=verbose, save_insqlite=True)
    
    logger.success(f"[+]{len(scrappedUnds)} future curves scrapped,  {len(resDB)} lines saved in DB")
    # generate the string to be used in the email
    if len(missingUnds) > 0:
        msg = f"Future curves scraped with some errors - {len(missingUnds)} missing"
        for el in missingUnds:
            msg += f"\n\tERROR with {el}"
        msg += "-----\n"
    else:
        msg = "Future curves scraped perfectly\n"

    msgDetails = ""
    for und in scrappedUnds:
        msg += f"\n\t{und}: {len(resDB[resDB.asset==und])} maturities scrapped"
    return (msg, msgDetails)


def CMEFUTS_last_date():
    return SQLA_last_date("FUTURES_CURVES")


ScrapCMEFuts = Scrap("FUTURES_CURVES", import_futs_curves, CMEFUTS_last_date)


if __name__ == "__main__":
    # print(refresh_data(True))
    # exit(0)
    import_futs_curves(True)
