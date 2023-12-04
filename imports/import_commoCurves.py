# %%
"""
DOes 2 things
   - imports DBC omposition from a csv file on the provider website
   - computes rolldown from various cuves, using now data previously scarpped from CME website by import_CMEfuts.py
"""
import os
import sys
import datetime

import pandas as pd
from io import StringIO
import requests

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)
MYPYTHON_ROOT = os.environ["ONEDRIVECONSUMER"] + "\\Python Scripts\\"
DIR_COMMOS = MYPYTHON_ROOT + "Finance\\Commos\\"
sys.path.append(DIR_COMMOS)

from credentials import QUANDL_KEY
from utils.utils import print_color, timer
from databases.classes import Scrap
from databases.database_mysql import SQLA_last_date, databases_update, SQLA_read_table
from common import last_bd
from commos_definitions import DEFINITIONS, URL_COMPO_DBC, DICT_NAMES, DICT_REPLACE_FUTURES

pd.options.mode.chained_assignment = None


def _download_from_file():
    csv_url = URL_COMPO_DBC
    response = requests.get(csv_url)
    df = None
    if response.status_code == 200:
        # Create a file-like object from the response content
        content = response.content
        content_type = response.headers["Content-Type"]

        # Check if the content is CSV
        if "text/csv" in content_type:
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        else:
            print("The URL does not point to a CSV file.")
    else:
        print("Failed to retrieve the CSV file from the URL.")

    if df is not None and len(df) > 0:
        df = df[df["Sector"] != "Collateral"]
        df = df.iloc[:, 1:]
    return df


def _clean_downloaded_data(df):
    df2 = df[["Name", "Contract Expiry Date", "Shares", "$ Value", "Weight"]]
    df2.columns = ["Security", "Expiry", "NOSH", "Value", "%NAV"]

    df2["Exch"] = df2["Security"].apply(lambda s: s.split()[0])
    df2["Name"] = df2["Security"].apply(lambda s: " ".join(s.split()[1:-1]))
    df3 = df2.groupby(["Name", "Security", "Exch"]).agg({"Expiry": "first", "NOSH": 'sum', "Value": 'sum', "%NAV": 'sum'})

    df3 = df3.reset_index()
    df3["Exchange"] = df3["Exch"]
    df3.drop("Exch", axis=1, inplace=True)
    df3["Name"] = df3["Name"].apply(lambda s: DICT_NAMES.get(s, s))
    df3["Date"] = last_bd
    df3.set_index("Name", inplace=True)

    return df3


@timer
def saveCompo_toDB(verbose=True) -> pd.DataFrame:
    """
    The function `saveCompo_toDB` downloads DBC compositions from a csv on the provider website, cleans it, prints the cleaned data if
    `verbose` is True, updates a database with the cleaned data, and returns the cleaned data.

    :return: the composition of DBC
    """

    df = _download_from_file()
    dfCompo = _clean_downloaded_data(df)
    if verbose:
        print_color("*** DBC latest weights ***", color="RESULT")
        print(dfCompo)
    databases_update(dfCompo.reset_index(), "COMPO_DBC", idx=False, mode="replace", verbose=verbose, save_insqlite=True)
    return dfCompo


def single_commofut_curve(df: pd.DataFrame, asset: str) -> dict:
    """The function `single_commofut_curve` calculates various metrics for a given asset using a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The parameter `df` is a pandas DataFrame that contains the data for multiple assets.
        Each row in the DataFrame represents a specific asset at a specific date.
    asset : str
        The "asset" parameter is a string that represents the asset for which you want to calculate the single commodity future curve.

    Returns
    -------
        a dictionary containing various values related to a specific asset. The dictionary includes the asset name, the roll values for different maturities (in %), and the settle values for the first 24 dates.

    """

    dictRes = {}
    dictRes["asset"] = asset

    dfAsset = df[(df["asset"] == asset)]
    dfAsset = dfAsset[dfAsset["Date"] == dfAsset["Date"].max()]
    dfAsset["Maturity"] = dfAsset["Expiry"].apply(lambda x: datetime.datetime.strptime(x, "%b %Y"))
    dfAsset["timetoFirst"] = dfAsset["Maturity"] - dfAsset["Maturity"].min()
    dfAsset["yearFrac"] = dfAsset["timetoFirst"].apply(lambda x: x.days / 365)
    for mats in [(1, 2), (2, 3), (1, 6)]:
        last = mats[1]
        first = mats[0]
        dictRes[f"Roll{mats[0]}-{mats[1]}"] = (
            (dfAsset["Settle"].iloc[last] - dfAsset["Settle"].iloc[first])
            / dfAsset["Settle"].iloc[0]
            * 100
            / (dfAsset["yearFrac"].iloc[last] - dfAsset["yearFrac"].iloc[first])
        )

    for i in range(1, 25):
        try:
            dictRes[str(i)] = dfAsset["Settle"].iloc[i]
        except:
            dictRes[str(i)] = None

    return dictRes


def import_commos_curves(verbose=True) -> pd.DataFrame:
    """The function imports DBC compo from the DB. It then computes the annualised roll for each future as well as the adjusted DBC weights"""

    dfDBCweights = SQLA_read_table("COMPO_DBC")
    dfCommos = dfDBCweights.loc[:, ["Name", "Security", "Exchange", "%NAV", "Date"]]
    dfCommos["toScrap"] = dfCommos["Name"].apply(lambda x: DICT_REPLACE_FUTURES.get(x, x))

    dfCommos.set_index("Name", inplace=True)
    if verbose:
        print_color("DBC weights:", "COMMENT")
        print(dfCommos)

    dfTS = SQLA_read_table("FUTURES_CURVES")
    assets = dfCommos["toScrap"].unique()
    tab = []
    for asset in assets:
        try:
            d = single_commofut_curve(dfTS, asset)
        except:
            print(f"[-]Could not read data for {asset} - using NaN")
            d = {"asset": asset}
        tab.append(d)
    df = pd.DataFrame(tab)

    # now computes normalised weights
    grouped = dfCommos.reset_index().groupby("toScrap").agg({"%NAV": sum})
    df["DBC Weight"] = df["asset"].apply(lambda x: grouped.loc[x, "%NAV"] if x in grouped.index else None)
    df["Date"] = last_bd
    df["Timestamp"] = last_bd
    df.sort_values("DBC Weight", ascending=False, inplace=True)
    if verbose:
        print_color("DBC commodities rolls:", "RESULT")
        print(df)

    return df


@timer
def save_rolldown_toDB(verbose=True) -> pd.DataFrame:
    df = import_commos_curves(verbose=verbose)
    databases_update(df, "COMMO_FUTURES_CURVES", idx=False, mode="replace", verbose=verbose, save_insqlite=True)
    return df


def save_all_commos_toDB(verbose=False) -> None:
    saveCompo_toDB(verbose=verbose)
    save_rolldown_toDB(verbose=verbose)


def import_commosCurves(verbose=True) -> str:
    """
    The function `import_commosCurves` downloads DBC composition data and commodity futures curves data from the web,
    saves the data into a database,
    and returns a message indicating the success or failure of the download.

    :return: a message string.
    """
    msg = None
    try:
        res = saveCompo_toDB(verbose=verbose)
        msg = f"DBC composition well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols"
    except Exception as e:
        msg = "Error while downloading DBC compo"
        print_color(msg, "FAIL")
        print(e)

    try:
        res = save_rolldown_toDB(verbose=verbose)
        msg += f"Commos curves well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols"
    except Exception as e:
        msg += "Error while commos future curves"
        print_color("Error while commos future curves", "FAIL")
        raise e

    return msg


def commosCurves_last_date():
    return SQLA_last_date("COMPO_DBC")


ScrapCommosCurves = Scrap("COMMOS CURVES", save_all_commos_toDB, commosCurves_last_date)


if __name__ == "__main__":
    # import_commos_curves()
    # import_commosCurves()
    save_all_commos_toDB()
