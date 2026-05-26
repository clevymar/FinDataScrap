"""
ETF Static Data Importer

Scrapes ETF metadata from etfdb.com using Selenium.
"""

import os
import re
import sys
import time
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

# Path setup - imports -> GCP -> ImportData -> FinDashboard
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)  # GCP
sys.path.append(parentdir)
rootdir = os.path.dirname(os.path.dirname(parentdir))  # FinDashboard
sys.path.append(rootdir)

import common_modules
from common_modules import ETF_unds, other_ETFS, basics, FIELDS
from scrap_selenium import start_driver
from databases.database_mysql import SQLA_read_table, databases_update

DIR_MAIN = common_modules.dir_output


def clean_string(s: str) -> str:
    """Remove newlines, carriage returns, and tabs from a string."""
    return re.sub(r'[\n\r\t]', "", s)


def get_today() -> str:
    """Get today's date as string."""
    return str(date.today())


def load_etf_db() -> pd.DataFrame:
    """Load the ETF_DB table from database."""
    return SQLA_read_table("ETF_DB").set_index("ETF")


# =============================================================================
# ETF Listing Functions
# =============================================================================

def list_existing_ETF(verbose: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create a summary DataFrame of all ETFs across different sources.

    Args:
        verbose: If True, print detailed lists

    Returns:
        Tuple of (summary_df, etf_db_df)
    """
    def add_column(df: pd.DataFrame, lst: list, name: str) -> pd.DataFrame:
        if not lst:
            logger.warning(f"No data for {name}")
            return df
        df2 = pd.DataFrame({"ETF": lst})
        df2[name] = 1
        df2.set_index("ETF", inplace=True)
        return pd.concat([df, df2], axis=1)

    # Collect ETFs from Python config
    python_etfs = sorted(set(basics + other_ETFS + ETF_unds))
    logger.info(f"{len(python_etfs)} ETFs in Python config")
    if verbose:
        print(python_etfs)

    # Load from ETFDB
    etf_db = load_etf_db()
    etfdb_names = sorted(etf_db.index.tolist())
    logger.info(f"{len(etfdb_names)} ETFs in ETF_DB")
    if verbose:
        print(etfdb_names)

    # Load from SPOTS
    spots_df = SQLA_read_table("EQTY_SPOTS")
    spots_names = sorted([c for c in spots_df.columns.tolist() if c != "Date"])
    logger.info(f"{len(spots_names)} ETFs in SPOTS")
    if verbose:
        print(spots_names)

    # Load from RATIOS
    ratios_df = SQLA_read_table("ETF_RATIOS")
    if "index" in ratios_df.columns:
        ratios_df = ratios_df.set_index("index")
    ratios_names = sorted(ratios_df.index.tolist())
    logger.info(f"{len(ratios_names)} ETFs in ETF_RATIOS")
    if verbose:
        print(ratios_names)

    # Build summary DataFrame
    df = etf_db[["Asset Class"]].copy()
    df["ETFDB"] = 1
    df = add_column(df, python_etfs, "Python")
    df = add_column(df, spots_names, "EQTY_SPOTS")
    df = add_column(df, ratios_names, "ETF_RATIOS")
    df.sort_values("Asset Class", inplace=True)

    return df, etf_db


def list_ETF_to_add() -> list[str]:
    """Find ETFs that need to be added to the database."""
    df_existing, _ = list_existing_ETF()
    existing = set(df_existing.index.tolist())

    new_etfs = _read_etf_list_file()
    to_add = [etf for etf in new_etfs if etf not in existing]
    logger.info(f"{len(to_add)} ETFs to add")
    return to_add


def _read_etf_list_file() -> list[str]:
    """Read ETF tickers from the etf_list.txt file."""
    file_path = os.path.join(DIR_MAIN, "etf_list.txt")
    if not os.path.exists(file_path):
        logger.warning(f"ETF list file not found: {file_path}")
        return []

    with open(file_path, "r") as f:
        content = f.readlines()

    etfs = []
    for line in content:
        match = re.search(r"\((.*?)\)", line)
        if match:
            etfs.append(match.group(1))

    return sorted(etfs)


# =============================================================================
# ETF Scraping Functions (Selenium-based)
# =============================================================================

def scrape_single_etf(driver, etf_name: str) -> pd.DataFrame:
    """
    Scrape ETF data from etfdb.com using Selenium.

    Args:
        driver: Selenium WebDriver instance
        etf_name: ETF ticker symbol

    Returns:
        DataFrame with ETF metadata
    """
    start = time.perf_counter()
    data: list[list] = [["ETF", etf_name]]

    url = f"https://etfdb.com/etf/{etf_name}/#etf-ticker-profile"
    driver.get(url)

    # Wait for page to load
    time.sleep(2)
    logger.info(f"Scraping {etf_name} from {url} - title: {driver.title}")

    html = driver.page_source
    # logger.debug(f"Page size: {len(html)} characters")
    soup = BeautifulSoup(html, "lxml")

    # Extract ETF name
    name_elem = soup.find("h1", {"class": "data-title"})
    if name_elem:
        full_name = clean_string(name_elem.text)[len(etf_name):]
        data.append(["Name", full_name])
        logger.info(f"ETF Name: {full_name}")

    # Extract sections
    _extract_section(soup, "Vitals", data)
    _extract_section(soup, "ETF Database Themes", data)
    _extract_factset_classifications(soup, data)
    _extract_section(soup, "Historical Trading Data", data, use_li=True)
    _extract_section(soup, "Trading Data", data, use_li=True)

    elapsed = time.perf_counter() - start
    data.append(["Time to run", elapsed])
    data.append(["Last update", get_today()])

    logger.info(f"Scraped {etf_name} in {elapsed:.1f}s")

    # Convert to DataFrame
    df = pd.DataFrame(data).T
    df.columns = df.iloc[0]
    df = df[1:].set_index("ETF")

    return df


def _extract_section(soup: BeautifulSoup, section_name: str, data: list[list], use_li: bool = False):
    """Extract data from a named section."""
    header = soup.find("h3", text=section_name)
    if not header or not header.parent:
        return

    table = header.parent
    if use_li:
        elements = table.find_all("li") if hasattr(table, "find_all") else []
    else:
        elements = table.find_all("div", {"class": "row"}) if hasattr(table, "find_all") else []

    for elem in elements:
        spans = elem.find_all("span")
        if len(spans) >= 2:
            field = spans[0].text
            if field in FIELDS:
                value = clean_string(spans[1].text)
                data.append([field, value])


def _extract_factset_classifications(soup: BeautifulSoup, data: list[list]):
    """Extract FactSet Classifications section."""
    header = soup.find("h3", text="FactSet Classifications")
    if not header or not header.parent:
        return

    parent = header.parent
    if not hasattr(parent, "find"):
        return

    tbody = parent.find("tbody")
    if not tbody or not hasattr(tbody, "find_all"):
        return

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            field = cells[0].text.strip()
            if field in FIELDS and field != "Category":
                value = clean_string(cells[1].text.strip())
                data.append([field, value])


# =============================================================================
# Database Update Functions
# =============================================================================

def update_etfs(etf_list: list[str], etf_db: pd.DataFrame | None = None) -> tuple[list[str], pd.DataFrame]:
    """
    Update ETF database with data from etfdb.com.

    Args:
        etf_list: List of ETF tickers to update
        etf_db: Existing ETF_DB DataFrame. If None, loads from database.

    Returns:
        Tuple of (errors list, updated etf_db DataFrame)
    """
    if etf_db is None:
        etf_db = load_etf_db()

    existing = etf_db.index.tolist()
    today = get_today()
    errors = []
    new_data = []
    total = len(etf_list)

    driver = start_driver(headless=True, forPA=True)

    try:
        for i, etf in enumerate(etf_list, 1):
            logger.info(f"Processing {etf} ({i}/{total})")

            # Skip if already updated today
            if etf in existing:
                last_update = str(etf_db.loc[etf, "Last update"])
                if last_update == today:
                    logger.info(f"  {etf} already updated today, skipping")
                    continue

            try:
                df = scrape_single_etf(driver, etf)

                # Remove old entry if exists
                if etf in existing:
                    etf_db = etf_db.drop(etf)

                new_data.append(df)

            except Exception as e:
                logger.error(f"Failed to scrape {etf}: {e}")
                errors.append(etf)

    finally:
        driver.quit()

    # Save results
    if new_data:
        new_df = pd.concat(new_data)
        etf_db = pd.concat([new_df, etf_db]).sort_values("ETF")
        etf_db.to_csv("static_ETF_DB.csv")
        # Reset index to avoid TEXT column as primary key in MySQL
        databases_update(etf_db.reset_index(), "ETF_DB", idx=False, verbose=False)
        logger.success(f"Updated {len(new_data)} ETFs in database")
    else:
        logger.info("No updates to save")

    if errors:
        logger.warning(f"Failed to update {len(errors)} ETFs: {errors}")

    return errors, etf_db


def refresh_all_etfs() -> list[str]:
    """
    Refresh data for all existing ETFs in the database.

    Returns:
        List of ETFs that failed to update
    """
    etf_db = load_etf_db()
    existing = etf_db.index.tolist()
    logger.info(f"Refreshing {len(existing)} ETFs")

    if len(existing)>0:
        errors, _ = update_etfs(existing, etf_db)

        if errors:
            logger.error(f"Failed to update: {errors}")
    else:
        logger.error("No ETFs found in database to refresh !!!!!!!!!\n Exiting...")
        errors = []

    return errors


def add_new_etfs(etf_list: list[str] | None = None) -> list[str]:
    """
    Add new ETFs to the database.

    Args:
        etf_list: List of ETFs to add. If None, uses default from common_modules.

    Returns:
        List of ETFs that failed to add
    """
    if etf_list is None:
        etf_list = common_modules.assets_coll.get("Mine", []) or []

    etf_db = load_etf_db()
    if len(etf_db) == 0:
        logger.warning("ETF_DB is empty")
        to_add = etf_list
    else:
        existing = set(etf_db.index.tolist())
        to_add = [etf for etf in etf_list if etf not in existing]

    if not to_add:
        logger.info("No new ETFs to add - they are already all present in the DB")
        return []

    logger.info(f"Adding {len(to_add)} new ETFs: {to_add}")
    errors, _ = update_etfs(to_add, etf_db)

    if errors:
        logger.error(f"Failed to add: {errors}")

    return errors


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    recap, _ = list_existing_ETF()
    print(recap)
    add_new_etfs(recap.index.tolist())
    # refresh_all_etfs()
