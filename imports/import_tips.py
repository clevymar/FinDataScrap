import os
import sys
import datetime
import time
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd
from io import BytesIO
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common import exceptions

from rich.console import Console

console = Console()

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap

# from scrap_selenium import start_driver, SeleniumError, WebDriverWait, EC, By, details_element, WebElement


DIR_DOWNLOAD = "D:\\Downloads"


@timer
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
                console.log("Could not extract number found for German tips")

        else:
            console.log("No data found for German tips")
    else:
        console.log(f"Failed to retrieve the German tips webpage. Status code: {response.status_code}")
    return None


def _load_latest_french_file():
    files = os.listdir(DIR_DOWNLOAD)
    # Get the most recently added file
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(DIR_DOWNLOAD, x)))
    console.log(f"Loading {latest_file}")
    df = pd.read_excel(os.path.join(DIR_DOWNLOAD, latest_file), skiprows=5)
    df.set_index(df.columns[0], inplace=True)
    lastDate = str(df.index[-1])[:10]
    realYield = float(df.iloc[-1, 3]) * 100
    res = (lastDate, realYield)
    console.print(res)
    return res


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
        except exceptions.TimeoutException as e:
            console.log("Cookies button not found after waitTime")

        return

    # Setup Chrome options
    chrome_options = Options()
    # Specify your download directory
    prefs = {"download.default_directory": DIR_DOWNLOAD}
    chrome_options.add_experimental_option("prefs", prefs)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")

    # Initialize the WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 20)

    try:
        # Open the webpage
        driver.get("https://www.aft.gouv.fr/en/oatis-key-figures#rendement")

        # click on cookie button if needed
        go_through_cookies()

        # Locate the download button (adjust the selector as per the actual page structure)
        download_button = driver.find_element(By.LINK_TEXT, "Téléchargez les données / Download the data")
        # Click the button to start the download
        download_button.click()

        # pb with checkbox ?
        try:
            holder = wait.until(EC.presence_of_element_located((By.CLASS_NAME,"ctp-checkbox-container")))
            print(holder)
            checkbox = driver.find_element(By.CSS_SELECTOR, 'div input[type="checkbox"]')
            print(checkbox)
            # checkbox = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#challenge-stage > div > label > input[type=checkbox]")))
            # checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="challenge-stage"]/div/label/input')))
            # checkbox = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'ctp-label')))
            """
            //*[@id="challenge-stage"]/div/label/input
            #challenge-stage > div > label > input[type=checkbox]
            ctp-label
            """
            # Click the checkbox
            checkbox.click()
            time.sleep(5)
            return True
        except exceptions.TimeoutException:
            print("Checkbox not found")
        except exceptions.ElementClickInterceptedException:
            print("Checkbox not clickable")
        except Exception as e:
            print(e)
    finally:
        driver.quit()
    return None


@timer
def french_tips():
    try:
        if _scrap_french_tips():
            res = _load_latest_french_file()
            return res
    except Exception as e:
        console.print_exception()
        raise Exception("Could not scrap French Tips") from e


def scrap_allTIPS():
    results = []
    prev_bd = pd.bdate_range(end=pd.Timestamp.now(), periods=2)[-2].strftime("%Y-%m-%d")
    try:
        yield_de = german_tips()
        temp = {"Country": "Germany", "Date_data": datetime.date.today().strftime("%Y-%m-%d"), "Real Yield": yield_de, "Date": prev_bd}
        results.append(temp)
    except Exception as e:
        console.print_exception()
        console.log("[-]Could not scrap German Tips")

    try:
        res = french_tips()
        if res:
            temp = {"Country": "France", "Date_data": res[0], "Real Yield": res[1], "Date": prev_bd}
            results.append(temp)
    except Exception as e:
        console.print_exception()
        console.log("[-]Could not scrap French Tips")

    print(results)
    if len(results) > 0:
        df = pd.DataFrame(results)
        return df


if __name__ == "__main__":
    # console.log("Importing German tips")
    # console.print(german_tips())
    df = scrap_allTIPS()
    print(df)
