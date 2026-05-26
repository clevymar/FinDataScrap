# ruff: noqa: E402

"""Import futures curves into the GCP finance database.

This module scrapes daily futures curves for the assets defined in the shared
``common/cme_definitions.py`` registry. Most exchange-traded contracts are read
from CME pages with Selenium: the scraper first tries each product's
``.quotes.html`` page, then falls back to the matching ``.settlements.html``
page when the quotes table has no usable settle prices. Settlement fallback rows
keep the database schema unchanged by setting ``Last`` equal to ``Settle``.

Some non-CME or unavailable CME products are routed to specialist importers:
Eurex/ICE/HKEX-style curves go through ``import_otherfuts``, while soft
commodities without a CME URL use OpenBB via ``import_futs_obb``. The final
result is normalized to ``Expiry``, ``Last``, ``Settle``, ``asset``, and
``Date`` before being appended to ``FUTURES_CURVES``.

The scraper logs the page type, full URL, cookie and "load all" handling, raw
and filtered row counts, and the final source used for each CME asset. Those
logs are intentionally verbose because CME page layouts and data availability
change often, and zero-row curves need to be diagnosable from the run log.
"""

import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

# Locally common/ is at Finance/ (4 levels up); on PA it's at FinDataScrap/ (already in sys.path)
import importlib.util
from pathlib import Path

_finance_root = Path(__file__).resolve().parents[4]
_cme_definitions_path = _finance_root / "common" / "cme_definitions.py"
if not _cme_definitions_path.exists():
    for sys_path_entry in sys.path:
        candidate = Path(sys_path_entry) / "common" / "cme_definitions.py"
        if candidate.exists():
            _cme_definitions_path = candidate
            break

if not _cme_definitions_path.exists():
    raise ImportError(f"Could not find CME definitions at {_cme_definitions_path} or on sys.path")

_cme_definitions_spec = importlib.util.spec_from_file_location("finance_cme_definitions", _cme_definitions_path)
if _cme_definitions_spec is None or _cme_definitions_spec.loader is None:
    raise ImportError(f"Could not load CME definitions from {_cme_definitions_path}")
_cme_definitions = importlib.util.module_from_spec(_cme_definitions_spec)
_cme_definitions_spec.loader.exec_module(_cme_definitions)
dictAssets = _cme_definitions.dictAssets

import time
import pandas as pd

from bs4 import BeautifulSoup
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from loguru import logger

from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from import_common import last_bd
from scrap_selenium import start_driver, _clean_price
from import_otherfuts import scrap_otherAsset
from import_futs_obb import get_futures_curve

# TODO add Eurex and CHF
# import eurex_curves
# import CHF_curve

# TODO  1) Zinc missing 2) replace CME for Sugar, Wheat...by correct exchange ?


STEM = "https://www.cmegroup.com/markets/"
PAGE_TAILS = {
    "quotes": ".quotes.html",
    "settlements": ".settlements.html",
}
BTN_XPATH = "/html/body/main/div/div[3]/div[2]/div/div/div/div/div/div[2]/div/div/div/div/div/div[5]/div/div/div/div[2]/div[2]/button"
BTN_XPATH = "/html/body/main/div/div[3]/div[3]/div/div/div/div/div/div[2]/div/div/div/div/div/div[6]/div/div/div/div[2]/div[2]/button"

BTN_COOKIES = '//*[@id="onetrust-accept-btn-handler"]'


# %%


def _get_webData(driver, asset: str, coreURL: str, page_type: str, clickCookies: bool = True):
    def _scroll_down(forCME: bool = True):
        if forCME:
            driver.find_element(By.XPATH, "//body").send_keys(Keys.PAGE_DOWN)
        else:
            driver.execute_script("window.scrollTo(0, 1000)")

    if page_type not in PAGE_TAILS:
        raise ValueError(f"Unsupported CME page type: {page_type}")

    url = STEM + coreURL + PAGE_TAILS[page_type]
    logger.info(f"[{asset}] Loading CME {page_type} page: coreURL={coreURL}, url={url}")
    driver.get(url)
    table = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, "main-table-wrapper")))
    logger.debug(f"[{asset}] Main table visible on {page_type} page: class={table.get_attribute('class')}")
    hasClickedCookies = False

    if clickCookies:
        logger.info(f"[{asset}] Looking for cookies button on {page_type} page")
        try:
            _scroll_down()
            python_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            logger.info(f"[{asset}] Clicking on cookies button")
            python_button.click()
            hasClickedCookies = True
        except Exception:
            logger.warning(f"[{asset}] [-] Cant find cookies button on {page_type} page")
    else:
        logger.debug(f"[{asset}] Skipping cookies lookup; cookie state already handled")

    _scroll_down()
    time.sleep(1)

    hasClickedLoadAll = False
    for attempt in range(1, 3):
        try:
            python_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, BTN_XPATH)))
            python_button.click()
            hasClickedLoadAll = True
            logger.info(f"[{asset}] Clicked LOAD ALL button on {page_type} page")
            time.sleep(2)
            break
        except TimeoutException:
            logger.warning(
                f"[{asset}] [-] Timeout getting LOAD ALL button on {page_type} page "
                f"(attempt {attempt}/2) - might just not exist; scrolling down again"
            )
            _scroll_down()
            time.sleep(1)
        except ElementClickInterceptedException:
            logger.warning(
                f"[{asset}] [-] Could not click LOAD ALL button on {page_type} page "
                f"(attempt {attempt}/2) - might just not exist; scrolling down again"
            )
            _scroll_down()
            time.sleep(1)
        except Exception as e:
            logger.exception(f"[{asset}] [-] Error processing LOAD ALL button on {page_type} page: {e}")

    if not hasClickedLoadAll:
        logger.debug(f"[{asset}] LOAD ALL button was not clicked on {page_type} page")

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find(attrs={"class": "main-table-wrapper"})
    if table is None:
        logger.warning(f"[{asset}] [-] Main table wrapper missing on {page_type} page: {url}")
        return None, hasClickedCookies

    table_body = table.find("tbody")
    if table_body is None:
        logger.warning(f"[{asset}] [-] No table body found on {page_type} page: {url}")
        return None, hasClickedCookies

    raw_rows = table_body.find_all("tr")
    logger.info(f"[{asset}] Found {len(raw_rows)} raw table rows on {page_type} page")
    return table_body, hasClickedCookies


def _process_results(table_body, asset: str, page_type: str):
    if table_body is None:
        logger.warning(f"[{asset}] Cannot process {page_type} page because table body is missing")
        return pd.DataFrame(columns=["Last", "Settle"]).rename_axis("Expiry")

    if page_type == "quotes":
        min_cols = 6
        last_col = 3
        settle_col = 5
    elif page_type == "settlements":
        min_cols = 7
        last_col = None
        settle_col = 6
    else:
        raise ValueError(f"Unsupported CME page type: {page_type}")

    res = []
    raw_rows = table_body.find_all("tr")
    short_rows = 0
    for row_num, tr in enumerate(raw_rows, start=1):
        els = tr.find_all("td")
        if len(els) < min_cols:
            short_rows += 1
            logger.warning(
                f"[{asset}] Skipping short {page_type} row {row_num}: "
                f"expected at least {min_cols} cells, found {len(els)}"
            )
            continue

        expiry = els[0].text[:8].strip()
        settle = _clean_price(els[settle_col].text.strip())
        last = settle if last_col is None else _clean_price(els[last_col].text.strip())
        tab = [
            expiry,
            last,
            settle,
        ]
        if len(res) < 3:
            logger.debug(
                f"[{asset}] Sample parsed {page_type} row {row_num}: "
                f"expiry={expiry}, last={last}, settle={settle}"
            )
        res.append(tab)

    dfFuts = pd.DataFrame(res, columns=["Expiry", "Last", "Settle"])
    raw_count = len(dfFuts)
    if raw_count == 0:
        logger.warning(f"[{asset}] No parsable rows found on {page_type} page; short_rows={short_rows}")
        return dfFuts.set_index("Expiry")

    dfFuts = dfFuts.dropna(subset=["Settle"])
    non_null_count = len(dfFuts)
    dfFuts = dfFuts[dfFuts["Settle"] > 0]
    final_count = len(dfFuts)
    logger.info(
        f"[{asset}] Parsed {page_type} rows: raw={len(raw_rows)}, parsable={raw_count}, "
        f"short={short_rows}, missing_settle_dropped={raw_count - non_null_count}, "
        f"non_positive_settle_dropped={non_null_count - final_count}, final={final_count}"
    )
    dfFuts = dfFuts.set_index("Expiry")
    return dfFuts


def scrap_asset(driver, asset: str, verbose=True, need_to_click_cookies=True):
    assetDef = dictAssets[asset]
    coreURL = assetDef.coreURL
    hasClickedCookies = False
    source = "none"
    if coreURL:
        dfFuts = pd.DataFrame(columns=["Last", "Settle"]).rename_axis("Expiry")
        try:
            table_body, hasClickedCookies = _get_webData(driver, asset, coreURL, "quotes", need_to_click_cookies)
        except Exception:
            logger.exception(f"[{asset}] [-] Error getting quotes data")
            table_body = None

        try:
            dfFuts = _process_results(table_body, asset, "quotes")
        except Exception:
            logger.exception(f"[{asset}] [-] Error processing quotes data")
            dfFuts = pd.DataFrame(columns=["Last", "Settle"]).rename_axis("Expiry")

        if len(dfFuts) > 0:
            source = "quotes"
        else:
            logger.warning(f"[{asset}] Quotes returned 0 usable rows; trying settlements fallback")
            try:
                table_body, clicked_settlement_cookies = _get_webData(
                    driver,
                    asset,
                    coreURL,
                    "settlements",
                    need_to_click_cookies and not hasClickedCookies,
                )
                hasClickedCookies = hasClickedCookies or clicked_settlement_cookies
            except Exception:
                logger.exception(f"[{asset}] [-] Error getting settlements data")
                table_body = None

            try:
                dfFuts = _process_results(table_body, asset, "settlements")
            except Exception:
                logger.exception(f"[{asset}] [-] Error processing settlements data")
                dfFuts = pd.DataFrame(columns=["Last", "Settle"]).rename_axis("Expiry")

            if len(dfFuts) > 0:
                source = "settlements"

        if verbose:
            if len(dfFuts) > 0:
                logger.success(f"{asset} - {assetDef.ticker} future curve scrapped from {source}")
            else:
                logger.warning(f"{asset} - {assetDef.ticker} returned 0 rows from quotes and settlements")
            print(dfFuts)
        else:
            if len(dfFuts) > 0:
                logger.success(
                    f"*** {asset} - {assetDef.ticker} future curve scrapped from {source} - {len(dfFuts)} rows***"
                )
            else:
                logger.warning(f"*** {asset} - {assetDef.ticker} future curve returned 0 rows***")
        logger.info(f"[{asset}] Final CME source used: {source}; rows={len(dfFuts)}")
        return dfFuts, hasClickedCookies
    else:
        logger.warning(f"[{asset}] No CME coreURL configured")
        return None, False


def compose_html_msg(messages):
    msg = "<h1>Scraping futures curves</h1><br>"
    for m in messages:
        msg += f"<p>{m}</p><br>"  # -------------------<br>
    return msg


def _scrap_mono(
    _type: str,
    tab: list,
    asset: str,
    driver,
    counter: int,
    totalAssets: int,
    need_to_click_cookies: bool,
    verbose: bool = True,
):
    try:
        if verbose:
            logger.info(f"\n\n****** Scrapping data for {asset} [{counter}/{totalAssets}] ")

        match _type:
            case "IRF":
                df = scrap_otherAsset(driver, asset, verbose=False)
            case "NYBOT":
                BBcode = dictAssets[asset].ticker
                df = get_futures_curve(BBcode)
            case _:
                df, hasClickedCookies = scrap_asset(
                    driver, asset, verbose=False, need_to_click_cookies=need_to_click_cookies
                )
                if df is None:
                    df = pd.DataFrame()
                if need_to_click_cookies:
                    need_to_click_cookies = not hasClickedCookies
                else:
                    need_to_click_cookies = False
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
    return need_to_click_cookies


def refresh_data(verbose: bool = True) -> pd.DataFrame:
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
                need_to_click_cookies = _scrap_mono(
                    "IRF", tab, asset, driver, counter, totalAssets, need_to_click_cookies, verbose=verbose
                )
            elif asset in ["Sugar", "Cocoa"]:
                need_to_click_cookies = _scrap_mono(
                    "NYBOT", tab, asset, driver, counter, totalAssets, need_to_click_cookies, verbose=verbose
                )
            else:
                need_to_click_cookies = _scrap_mono(
                    "CME", tab, asset, driver, counter, totalAssets, need_to_click_cookies, verbose=verbose
                )
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

    databases_update(
        resDB.reset_index(), "FUTURES_CURVES", idx=False, mode="append", verbose=verbose, save_insqlite=True
    )

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
        msg += f"\n\t{und}: {len(resDB[resDB.asset == und])} maturities scrapped"
    return (msg, msgDetails)


def CMEFUTS_last_date():
    return SQLA_last_date("FUTURES_CURVES")


ScrapCMEFuts = Scrap("FUTURES_CURVES", import_futs_curves, CMEFUTS_last_date)


if __name__ == "__main__":
    # print(refresh_data(True))
    # exit(0)
    import_futs_curves(True)
