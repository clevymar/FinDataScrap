import os
import sys
from io import StringIO
import time

import pandas as pd
from rich.console import Console
from icecream import ic

console = Console()

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update
from databases.classes import Scrap
from scrap_selenium import start_driver, SeleniumError, WebDriverWait, EC, By, details_element, WebElement
from common import last_bd

IRS_ccies = ["EUR", "USD", "JPY", "CHF", "GBP"]


def scrap_allIRS(verbose=True):

    def btnAccept_click():
        btnAccept = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".btn--primary")))
        if btnAccept:
            if verbose:
                console.log("Button Accept found")
            btnAccept.click()
            if verbose:
                console.log("Button Accept clicked")
        else:
            if verbose:
                console.log("Button Accept not found")

    def go_through_cookies(verbose: bool = True) -> None:
        btnCookies = wait.until(EC.presence_of_element_located((By.ID, "popin_tc_privacy_button")))
        if btnCookies:
            if verbose:
                console.log("Cookies button found")
            btnCookies.click()
            if verbose:
                console.log("Cookies button clicked")
        else:
            if verbose:
                console.log("Cookies button not found")

        # * deal with accept
        time.sleep(1)
        chkBoxAccept = wait.until(EC.presence_of_element_located((By.XPATH, '//div[@role="checkbox"]')))
        if chkBoxAccept:
            if verbose:
                console.log("Checkbox found")
            chkBoxAccept.click()
            if verbose:
                console.log("Checkbox clicked")
        else:
            console.log("Checkbox not found")
            btnAccept_click


        # click on top of screen otherwise creates issues
        try:
            btnBS = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/main/div[2]/div/nav/div[1]/div/div/ul/li[3]/a")))
            btnBS.click()
        except:
            console.log("Could not click on top of screen - a priori should be ok")

    def scrap_ccy(section: WebElement, button: WebElement, verbose: bool = True) -> WebElement:
        """
        Scrapes currency data by interacting with web elements on a page.
        Args:
            section (WebElement): The section of the webpage containing the buttons.
            button (WebElement): The button element to be clicked for scraping.
            verbose (bool, optional): If True, prints detailed logs. Defaults to True.
        Returns:
            WebElement: The updated section element after interaction.
        Raises:
            Exception: If an error occurs during the interaction with the "Show more" button.
        """
        def handle_showMore(btnShowMore: WebElement):
            if verbose:
                console.log(f"Button Show More for {CCY} found")
            btnShowMore.click()
            if verbose:
                console.log("Button Show More clicked")
            time.sleep(SLEEP_TIME)

        CCY = button.text
        if verbose:
            console.log(f"Processing {CCY}")
        time.sleep(SLEEP_TIME)
        driver.execute_script("arguments[0].scrollIntoView();", button)
        driver.execute_script("arguments[0].click();", button)
        time.sleep(SLEEP_TIME)
        if verbose:
            console.log(f"Button ccy {CCY} clicked")

        try:
            # buttons = section.find_elements(By.TAG_NAME, "button")
            buttons = section.find_elements(By.CSS_SELECTOR, '[data-testid="show-more"]')
            foundButton = False
            if buttons[-1].text == "Show more":
                btnShowMore = buttons[-1]
                foundButton = True
                handle_showMore(btnShowMore)
            else:
                for button in buttons:
                    if button.text == "Show more":
                        btnShowMore = button
                        foundButton = True
                        handle_showMore(btnShowMore)
                        break

            if not foundButton:
                if verbose:
                    console.log(f"Button Show More for {CCY} not found")
        except Exception as e:
            if verbose:
                console.log(f"Button Show More for {CCY} not found\n\t {e}")
        # div = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "gem-comp-mio-api-table")))
        time.sleep(2)
        section = driver.find_element(By.ID, "3")
        return section

    def create_df_from_section(section: WebElement) -> pd.DataFrame:
        ic(section.text)
        s = section.text.replace("IRS\n", "IRS ")
        s = s.split("\n")[11:-1]
        s = "\n".join(s)
        data_io = StringIO(s)
        df = pd.read_csv(data_io, sep="\s+", engine="python", header=None)
        df = df.iloc[:, [0, 1, 3, -2]]
        df.columns = ["CCY", "Tenor", "Rate", "Date"]
        df = df[["Date", "CCY", "Tenor", "Rate"]]
        # df["Date"] = pd.to_datetime(df["Date"].apply(lambda s: s.replace(",", "")), dayfirst=True).dt.strftime("%Y-%m-%d")
        df["Date"] = last_bd

        return df

    SLEEP_TIME = 2
    URL = "https://www.sparkasse.at/investments-en/markets/market-overview/money-market-fixed-income"
    BTN_SHOW_MORE = (
        "/html/body/div[2]/main/div[2]/div/div[4]/section/div[2]/div/section/div/div/div[2]/div/div[1]/div/div[3]/div/div/div/div/div/div[2]/button"
    )

    driver = start_driver(headless=True, forCME = True)
    # driver = start_driver(headless=False, forCME = False)
    driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1',

        }
    })
    wait = WebDriverWait(driver, 5)
    try:
        driver.get(URL)
        try:
            go_through_cookies(verbose=verbose)
        except Exception as e:
            console.log(f"Could not deal with cookies") #{e}
            btnAccept_click()
            try:
                go_through_cookies(verbose=verbose)
            except Exception as e:
                console.log(f"Could not deal with cookies a second time")

        section = wait.until(EC.visibility_of_element_located((By.ID, "3")))
        time.sleep(SLEEP_TIME)
        ic(section)
        ic(section.text)

        buttons = section.find_elements(By.XPATH, ".//button[@role='tab']")
        time.sleep(SLEEP_TIME)
        if verbose:
            console.log(f"\n\n{len(buttons)} currency buttons found: " + ",".join([b.text for b in buttons]))
        res = []
        for button in buttons:
            try:
                section = scrap_ccy(section, button, verbose=verbose)
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

    except Exception as e:
        console.print_exception()
        raise Exception(f"Could not scrap the link at {URL}") from e
    finally:
        console.log("Quitting Selenium driver", "INFO")
        driver.quit()

    return res


@timer
def IRS_toDB(verbose=False):
    res = scrap_allIRS(verbose)
    databases_update(res, "IRS_TS", idx=False, mode="append", verbose=verbose, save_insqlite=True)
    return res


@timer
def import_swaps(verbose=True):
    try:
        res = IRS_toDB()
        msg = f"Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols"
        return msg
    except Exception as e:
        print("Error while downloading")
        print(e)
        return "Error while downloading"


def swaps_last_date():
    return SQLA_last_date("IRS_TS")


ScrapIRS = Scrap("IRS", IRS_toDB, swaps_last_date)


if __name__ == "__main__":
    print(IRS_toDB(verbose=True))
