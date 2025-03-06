import os
import sys
import datetime
import time
from pathlib import Path
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd
from io import BytesIO
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common import exceptions
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import read_version_from_cmd
from webdriver_manager.core.os_manager import PATTERN

from tqdm import tqdm
from icecream import ic
from rich.console import Console

console = Console()

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from utils.utils import timer, isLocal
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap

# from scrap_selenium import start_driver, SeleniumError, WebDriverWait, EC, By, details_element, WebElement


DIR_DOWNLOAD = Path(__file__).parent / "Files"
STYLE_ERROR = "bold red"

VERSION_CHROME = 133

def german_tips() -> float | None:
    # URL of the page
    url = "https://www.deutsche-finanzagentur.de/en/federal-securities/factsheet/isin/DE0001030575/"

    # Fetch the page
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the element containing the desired data
        # This is a placeholder - you'll need to replace this with the correct selector
        data_element = soup.find_all(class_="bb-text-with-label__text big")

        if data_element:
            # Extract and print the data
            data = data_element[-1].get_text().strip()
            try:
                data = float(data.replace("%", ""))
                return data
            except:
                console.log("Could not extract number found for German tips", style=STYLE_ERROR)

        else:
            console.log("No data found for German tips", style=STYLE_ERROR)
    else:
        console.log(f"Failed to retrieve the German tips webpage. Status code: {response.status_code}", style=STYLE_ERROR)
    return None


""" now French """


def clean_folder():
    files = os.listdir(DIR_DOWNLOAD)
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(DIR_DOWNLOAD, x)))
    for f in files:
        if f != latest_file:
            os.remove(os.path.join(DIR_DOWNLOAD, f))
    files = os.listdir(DIR_DOWNLOAD)
    if len(files) > 1:
        console.log(f"[-]Too many files remaining in folder: {files}")
    else:
        console.log(f"Only file remaining in folder: {files[0]}", style=STYLE_ERROR)
    return


def has_file_been_downloaded() -> str | bool:
    files = os.listdir(DIR_DOWNLOAD)
    # Get the most recently added file
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(DIR_DOWNLOAD, x)))
    if latest_file.endswith(".xls") or latest_file.endswith(".xlsx"):
        current_time = time.time()
        creation_time = os.path.getctime(os.path.join(DIR_DOWNLOAD, latest_file))
        if (current_time - creation_time) <= 30:
            console.log(f"{latest_file} has been downloaded {current_time - creation_time:.0f} seconds ago")
            return latest_file
        else:
            console.log(f"File is too old - {latest_file} has been downloaded {current_time - creation_time:.1f} seconds ago", style=STYLE_ERROR)
    else:
        console.log("No xls file found", style=STYLE_ERROR)
    return False


def _load_latest_french_file() -> tuple[str, float] | None:
    console.log("Loading latest file")
    if latest_file := has_file_been_downloaded():
        console.log(f"Loading data from file")
        df = pd.read_excel(os.path.join(DIR_DOWNLOAD, latest_file), skiprows=5)
        df.set_index(df.columns[0], inplace=True)
        lastDate = str(df.index[-1])[:10]
        realYield = float(df.iloc[-1, 3]) * 100
        res = (lastDate, realYield)
        # console.print(res)
        try:
            clean_folder()
        except:
            console.log("Could not clean folder", style=STYLE_ERROR)
        return res
    else:
        return None


def _scrap_french_tips(verbose: bool = True) -> bool | None:
    def go_through_cookies(verbose: bool = True) -> None:
        # btnCookies = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "agree-button eu-cookie-compliance-secondary-button")))
        try:
            btnCookies = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#popup-buttons > button.agree-button.eu-cookie-compliance-secondary-button"))
            )
            if btnCookies:
                if verbose:
                    console.log("Cookies button found")
                btnCookies.click()
                if verbose:
                    console.log("Cookies button clicked")
            else:
                if verbose:
                    console.log("Cookies button not found")
        except exceptions.TimeoutException:
            console.log("Cookies button not found after waitTime", style=STYLE_ERROR)
        except exceptions.ElementClickInterceptedException:
            console.log("Cookies button not clickable", style=STYLE_ERROR)
        except Exception as e:
            console.log(f"Error while trying to click on cookies button: {e}", style=STYLE_ERROR)
        return

    def pb_with_cloudflare_checkbox():
        try:
            wait.until(
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe[title='Widget containing a Cloudflare security challenge']"))
            )
            holder = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ctp-checkbox-container")))
            ic(holder.text)
            checkbox = driver.find_element(By.CSS_SELECTOR, 'div input[type="checkbox"]')
            ic(checkbox.text)
            # Click the checkbox
            checkbox.click()
            console.log("Cloudflare button clicked")
            for _ in tqdm(range(30)):
                time.sleep(1)
            return True
        except exceptions.TimeoutException:
            console.log("Cloudflare checkbox not found", style=STYLE_ERROR)
        except exceptions.ElementClickInterceptedException:
            console.log("Cloudflare checkbox not clickable", style=STYLE_ERROR)
        except Exception as e:
            console.print_exception()


    # Setup Chrome options
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    prefs = {"download.default_directory": str(DIR_DOWNLOAD.resolve())}
    chrome_options.add_experimental_option("prefs", prefs)
    
    console.log(f"Starting browser version {VERSION_CHROME}")
    if isLocal():
        # Force download the latest ChromeDriver version
        # console.log("Downloading current browser...")
        # chrome_driver_path = ChromeDriverManager().install()
        # console.log(f"[+]Downloaded @ {chrome_driver_path}")
        try:
            # should download latest version automaticall
            driver = uc.Chrome(version_main=VERSION_CHROME,headless=False, options=chrome_options)  
            console.log("[+]Browser started successfully")
        except Exception as e:
            console.log(e) 
            console.log("Could not start the browser - check if you dont need to update local Chrome browser", style=STYLE_ERROR)
            exit(0)
            
    else:
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # * Initialize the WebDriver - force the version otherwise PythonAnywhere wont work
        driver = uc.Chrome(version_main=90, options=chrome_options)  # ,use_subprocess=False

    wait = WebDriverWait(driver, 20)

    try:
        # Open the webpage
        console.log("Opening page for French tips")
        driver.get("https://www.aft.gouv.fr/en/oatis-key-figures#rendement")

        # click on cookie button if needed
        go_through_cookies()

        # Click the button to start the download
        download_button = driver.find_element(By.LINK_TEXT, "Téléchargez les données / Download the data")
        console.log("Download button found - clicking")
        download_button.click()
        time.sleep(15)
        if has_file_been_downloaded():
            return True
        # pb with checkbox ?
        pb_with_cloudflare_checkbox()

    except exceptions.TimeoutException:
        console.log("Timeout", style=STYLE_ERROR)
    except Exception as e:
        console.print_exception()

    finally:
        driver.quit()
    return None


def french_tips() -> tuple[str, float] | None:
    try:
        if _scrap_french_tips():
            res = _load_latest_french_file()
            return res
    except Exception as e:
        console.print_exception()
        raise Exception("Could not scrap French Tips") from e
    return None


def scrap_allTIPS() -> pd.DataFrame:
    results = []
    prev_bd = pd.bdate_range(end=pd.Timestamp.now(), periods=2)[-2].strftime("%Y-%m-%d")
    console.log("Importing German tips")
    try:
        yield_de = german_tips()
        temp = {"Country": "Germany", "Date_data": datetime.date.today().strftime("%Y-%m-%d"), "Real Yield": yield_de, "Date": prev_bd}
        results.append(temp)
    except Exception:
        console.print_exception()
        console.log("[-]Could not scrap German Tips", style=STYLE_ERROR)

    console.log("Importing French tips")
    try:
        res = french_tips()
        if res:
            temp = {"Country": "France", "Date_data": res[0], "Real Yield": res[1], "Date": prev_bd}
            results.append(temp)
    except Exception:
        console.print_exception()
        console.log("[-]Could not scrap French Tips", style=STYLE_ERROR)

    if len(results) > 0:
        df = pd.DataFrame(results)
        return df
    return pd.DataFrame()


@timer
def tips_toDB(verbose=False) -> pd.DataFrame:
    res = scrap_allTIPS()
    if len(res) > 0:
        if verbose:
            print(res)
        databases_update(res, "TIPS_TS", idx=False, mode="append", verbose=verbose, save_insqlite=True)
    else:
        console.log("No TIPS data to update", style=STYLE_ERROR)
        res = pd.DataFrame()
    return res


def import_TIPS(argument=None):
    msg = None
    try:
        res = tips_toDB(True)
        if len(res) > 0:
            msg = f"Well downloaded !!! \n{len(res)} rows"
        else:
            msg = "No TIPS data to update"
    except Exception as e:
        raise Exception("Error while downloading TIPS") from e
    return msg


def tips_last_date():
    return SQLA_last_date("TIPS_TS")


ScrapTIPS = Scrap("TIPS", tips_toDB, tips_last_date)


if __name__ == "__main__":
    import_TIPS()
