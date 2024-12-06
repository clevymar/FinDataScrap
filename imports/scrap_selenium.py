import requests
import json
import time
import random
import os
import sys

from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from loguru import logger

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import TimeoutException


currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from common import last_bd, fichierTSUnderlyings
from utils.utils import timer
from databases.database_mysql import SQLA_read_table

COLS_MORNINGSTAR = [
    "ETF",
    "P/E1",
    "P/B",
    "P/S",
    "P/CF",
    "DY",
    "EG",
    "HG",
    "SG",
    "CFG",
    "BG",
    "Composite",
    "Last_updated",
    "UpdateMode",
    "URL",
    "Name",
]
COLS_RATIOS = ["EY", "B/P", "S/P", "DY", "Compo_Zscore"]
SECTORS = [
    "Basic Materials",
    "Consumer Cyclical",
    "Financial Services",
    "Real Estate",
    "Communication Services",
    "Energy",
    "Industrials",
    "Technology",
    "Consumer Defensive",
    "Healthcare",
    "Utilities",
]


class SeleniumError(Exception):
    pass


benchmark = "ACWI"
dictInput = json.load(open(fichierTSUnderlyings, "r"))
fullList = dictInput["EQTY_SPOTS"]

# ratios = []
initDict = {x: "" for x in COLS_MORNINGSTAR}
errs: list[str] = []


def _clean_price(s):
    try:
        if s == "-":
            return 0
        s = s.replace("'", ".")
        return float(s[:5])
    except Exception as e:
        logger.error(str(e))
        return None


def details_element(element: WebElement):
    id = element.get_attribute("id")
    logger.info(f"ID: {id}")

    # Get its name
    name = element.get_attribute("name")
    logger.info(f"Name: {name}")

    # Get its text
    text = element.text
    logger.info(f"Text: {text}")

    # Get its class
    class_name = element.get_attribute("class")
    logger.info(f"Class: {class_name}")

    # Get its tag name
    tag_name = element.tag_name
    logger.info(f"Tag Name: {tag_name}")


""" dealing with captcha"""


def find_cookie_file(file_path: str = "morningstar_cookies.txt"):
    if os.path.exists(file_path):
        pass
    else:
        file_path = f"imports/{file_path}"
        if os.path.exists(file_path):
            pass
        else:
            return None
    return file_path


def get_cookies_from_file(fichier: str = "morningstar_cookies.txt"):
    def convert_to_dict(line):
        line = line.replace("'", '"').replace("False", "false").replace("True", "true")
        return json.loads(line)

    file_path = find_cookie_file(fichier)
    if file_path is None:
        logger.error("cant find cookie file - proceeeding without")
        return None

    with open(file_path, "r") as file:
        content = file.read()

    cookie_strings = content.split("},{")
    cookie_strings[0] = cookie_strings[0] + "}"
    for i in range(1, len(cookie_strings) - 1):
        cookie_strings[i] = "{" + cookie_strings[i] + "}"
    cookie_strings[-1] = "{" + cookie_strings[-1]

    # Convert each split string to a dictionary and print the elements
    res = []
    for cookie_string in cookie_strings:
        cookie_dict = convert_to_dict(cookie_string.strip())
        res.append(cookie_dict)
    return res


def proc_to_get_manually_cookies(fichier: str = "temp_cookies.txt"):
    driver = start_driver(headless=False)
    url = "https://www.morningstar.com/etfs/arcx/DGS/portfolio"
    driver.get(url)
    try:
        btn = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "amzn-captcha-verify-button")))
        btn.click()
    except:
        logger.warning("Button not found")
    time.sleep(15)

    cookies = driver.get_cookies()
    logger.info(cookies)
    with open(fichier, "w") as file:
        file.write(str(cookies))
    driver.quit()


def hack_captcha(driver):
    cookies = get_cookies_from_file()
    if cookies:
        driver.execute_cdp_cmd("Network.enable", {})
        for cookie in cookies:
            # Fix issue Chrome exports 'expiry' key but expects 'expire' on import
            if "expiry" in cookie:
                cookie["expires"] = cookie["expiry"]
                del cookie["expiry"]
            # Set the actual cookie
            res = driver.execute_cdp_cmd("Network.setCookie", cookie)

        # Disable network tracking
        driver.execute_cdp_cmd("Network.disable", {})
        logger.info("\n cookies loaded")
        time.sleep(1)

    # print(driver.get_cookies())
    return


""" dealing with captcha - END """


def start_driver(headless: bool = True, forCME: bool = False, forMorninstar: bool = False):
    """
    The function `start_driver` creates a headless Chrome driver with specific options"""
    from selenium import webdriver

    try:
        driver = None
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--no-sandbox")
        if forCME:
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"
            )
            chrome_options.add_argument("--window-size=1920,1080")
            # chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--headless=new")
        else:
            if headless:
                chrome_options.add_argument("--headless")
            if forMorninstar:
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option("useAutomationExtension", False)
                chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--enable-unsafe-swiftshader")

        driver = webdriver.Chrome(options=chrome_options)

        return driver
    except Exception as e:
        if driver:
            driver.quit()
        logger.exception("Could not create the driver")
        raise Exception("Could not create the driver") from e


def _get_url(ETF_name: str, exchange="arcx", verbose=True):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    session = requests.Session()
    session.headers.update(headers)

    if exchange[:5] == "funds":
        url = f"https://www.morningstar.com/{exchange}/{ETF_name}/portfolio"
    else:
        foundURL = False
        EXCHANGE_LIST = [exchange] + [c for c in ["arcx", "xnys", "xnas", "bats"] if c != exchange]
        for exc in EXCHANGE_LIST:
            url = f"https://www.morningstar.com/etfs/{exc}/{ETF_name}/portfolio"
            r = session.get(url)
            if r.status_code in [200, 202]:
                foundURL = True
                exchange = exc
                break
            else:
                pass
                # print(f'Could not get a positive answer at {url} - {r.status_code}')
        if not foundURL:
            raise SeleniumError("Correct Url could not be found for: " + ETF_name)
    if verbose:
        logger.info(f"Scrapping for {ETF_name} on {exchange=} at {url=}")
    return url


def _sub_getETF_Selenium(driver, ETF_name: str, exchange="arcx", verbose=True):
    def find_table(class_name: str):
        for i in range(max_attempts):
            try:
                _ = WebDriverWait(driver, 3 * (i + 1)).until(EC.presence_of_element_located((By.CLASS_NAME, class_name)))
                hasAnythingWorked = True
                return True  # Exit the loop if successful
            except TimeoutException:
                if i < max_attempts:
                    logger.info(f"\t[-]]Retrying finding {class_name} for {ETF_name} attempt {i+2}")
                else:
                    logger.error(f"\t[-]Could not scrap {class_name} for {ETF_name}")
                continue  # Retry if a timeout occurs
        return False

    url = _get_url(ETF_name, exchange=exchange, verbose=verbose)
    driver.get(url)
    max_attempts = 3
    hasAnythingWorked = False

    # need to run first befor loading into BS
    hasValuation = find_table("sal-measures__value-table")
    hasSectors = find_table("sal-sector-exposure__sector-table")

    html_source = driver.page_source
    source_data = html_source.encode("utf-8")
    soup = BeautifulSoup(source_data, "lxml")

    compteur = 0
    row_dict = initDict.copy()
    row_dict["ETF"] = ETF_name
    row_dict["Last_updated"] = last_bd
    try:
        res = soup.find(class_="mdc-security-header__name")
        nom = res.text
        tmp = nom.splitlines()
        tmp = [el for el in tmp if len(el) >= 3]
        row_dict["Name"] = tmp[0].strip()
    except:
        logger.error(f"\t[-]Could not scrap name for {ETF_name}")

    """ find and grab financial ratios """
    if hasValuation:
        res = soup.find(class_="sal-measures__value-table")
        res = res.find("tbody")
        for row in res.find_all("tr"):
            sratio = row.find_all("td")[1].text
            if sratio != "":
                try:
                    ratio = float(sratio)
                except:
                    """exception for P/CF not as important as others, replace by cat"""
                    if row.find_all("td")[0].text.strip() == "Price/Cash Flow":
                        try:
                            ratio = float(row.find_all("td")[2].text)
                        except:
                            ratio = float("nan")
                    else:
                        ratio = float("nan")
                row_dict[COLS_MORNINGSTAR[compteur + 1]] = ratio
                compteur += 1
    # tbs=soup.findAll("tr", {"class": "ng-scope"})
    """ find and grab sector composition """
    if hasSectors:
        res = soup.find(class_="sal-sector-exposure__sector-table")
        res = res.find("tbody")
        for row in res.find_all("tr"):
            cells = row.find_all("td")
            lbl = cells[0].text.strip()
            if lbl in SECTORS:
                try:
                    weight = float(cells[1].text)
                except:
                    weight = float("nan")
                row_dict[lbl] = weight
                compteur += 1

    if hasAnythingWorked:
        row_dict["Last_updated"] = last_bd
        row_dict["UpdateMode"] = "Selenium"
        row_dict["URL"] = url
        return row_dict
    else:
        return None


@timer
def selenium_scrap_ratios(secList: list, verbose=True):
    """
    The function `selenium_scrap_ratios` uses Selenium to scrape ratios for a list of securities, and
    returns a DataFrame of the scraped data along with any errors encountered during scraping.

    :param secList: The `secList` parameter is a list of securities for which you want to scrape ratios
    using Selenium. Each security in the list should be a string representing the security symbol or
    identifier
    :return: The function `selenium_scrap_ratios` returns two values: `df` and `errs`. `df` is a pandas
    DataFrame containing the scrapped data for the underlyings ratios. `errs` is a list of underlyings
    for which there were errors in scrapping with Selenium.
    """
    # using info from https://help.pythonanywhere.com/pages/selenium
    res = []
    errs = []
    df = pd.DataFrame()
    dfETFRef = SQLA_read_table("ETF_REF")

    driver = start_driver(headless=True, forMorninstar=True)
    hack_captcha(driver)
    try:
        for sec in tqdm(secList):
            exc = "arcx"
            if sec in dfETFRef["ETF"].tolist():
                if dfETFRef.loc[dfETFRef["ETF"] == sec, "DoNotRatio"].iloc[0]:
                    continue
                elif (special := dfETFRef.loc[dfETFRef["ETF"] == sec, "specialRatio"].iloc[0]) != "0":
                    exc = special.split("_")[0]
            try:
                tmp = _sub_getETF_Selenium(driver, sec, exchange=exc, verbose=verbose)
                if tmp:
                    res.append(tmp.copy())
                else:
                    logger.error(f"[-]Error in scrapping with selenium for {sec} - EMPTY")
                errs.append(sec)
            except TimeoutException:
                logger.error(f"[-]Error in scrapping with selenium for {sec} - TIMEOUT")
                errs.append(sec)

            except Exception:
                logger.exception(f"[-]Error in scrapping with selenium for {sec}")
                errs.append(sec)

        if len(res) > 0:
            df = pd.DataFrame(res)
            logger.success(f"{len(df)} underlyings ratios scrapped")
            if verbose:
                print(df)
        else:
            logger.warning(f"[-]No data was scrapped with selenium")

        if len(errs) > 0:
            logger.warning(f"[-]Errors in scrapping with selenium for {len(errs)} underlyings")
            logger.error(errs)
    finally:
        logger.info("Quitting Selenium driver")
        driver.quit()
    return df, errs


if __name__ == "__main__":
    pass
