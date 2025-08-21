import datetime
import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

import pandas as pd
import numpy as np
from icecream import ic
from loguru import logger

from common import need_reimport, isLocal, last_bd
from utils.utils import timer
from scrap_selenium import selenium_scrap_ratios
from databases.database_mysql import SQLA_last_date, databases_update, PADB_connection
from databases.classes import Scrap

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

MAX_TOUPDATE = int(5 * 60 / 6)  # * around 6 secs per underlying, want to limit to 5 mins
# MAX_TOUPDATE = 2

benchmark = "ACWI"
# dictInput = json.load(open(fichierTSUnderlyings, "r"))
# secList = dictInput["EQTY_SPOTS"]


# ratios=[]
row_dict = {x: "" for x in COLS_MORNINGSTAR}
errs = []
pd.set_option("mode.chained_assignment", None)


def regenerate_ETF_list() -> list:
    existing = pd.read_csv("/home/CyrilFinanceData/FinDataScrap/imports/ratios_20231130.csv")
    etfs = existing["index"].tolist()  # + [benchmark] #TODO change sample away
    return etfs


def _compute_extra_ratios(ratios: pd.DataFrame):
    if len(ratios) > 0:
        tab = pd.DataFrame(ratios, columns=COLS_MORNINGSTAR + SECTORS)
        tab = tab.set_index("ETF")
        tab_ratios = tab[["P/E1", "P/B", "P/S", "P/CF", "DY", "Composite", "Last_updated", "UpdateMode", "URL", "Name"]]
        inverter = lambda x: 1 / x
        pctshow = lambda x: 100 * x
        for x in ["P/E1", "P/B", "P/S", "P/CF"]:
            tab_ratios.loc[:, x] = tab_ratios.loc[:, x].map(inverter)
        for x in ["P/E1", "P/CF"]:
            tab_ratios.loc[:, x] = tab_ratios.loc[:, x].map(pctshow)

        tab_ratios.columns = ["EY", "B/P", "S/P", "CFY", "DY", "Compo_Zscore", "Last_updated", "UpdateMode", "URL", "Name"]
        for col in ["EY", "B/P", "S/P", "CFY", "DY"]:
            col_z = col + "_Zscore"
            tab_ratios.loc[:, col_z] = np.nan
        tab_ratios.loc[:, "Compo_Zscore"] = np.nan
        # * create composite table """
        tab.drop(["DY", "Last_updated", "UpdateMode", "URL", "Name"], axis=1, inplace=True)
        tabf = pd.concat([tab_ratios, tab], axis=1)
        return tabf


def _prep_ratios(ratios: pd.DataFrame, dfExisting: pd.DataFrame) -> pd.DataFrame | None:
    if len(ratios) > 0:
        res = _compute_extra_ratios(ratios)
        res = res.reset_index().rename(columns={"ETF": "index"})
        res["Date"] = res["Last_updated"]
        updatedUnds = res["index"].tolist()
        if dfExisting is not None:
            dfOld = dfExisting[~dfExisting["index"].isin(updatedUnds)].drop("need_reimport", axis=1)
            dfNew = pd.concat([dfOld, res], axis=0).reset_index(drop=True)
        else:
            dfNew = res
        # * now recreates the composite table
        # TODO should I do it only when ACWI updated ?
        for col in ["EY", "B/P", "S/P", "CFY", "DY"]:
            # dfNew[col] = dfNew[col].astype(float)
            col_z = col + "_Zscore"
            avg = dfNew.loc[dfNew["index"] == benchmark, col].iloc[0]
            standev = dfNew[col].std()
            dfNew[col_z] = (dfNew[col] - avg) / standev
        dfNew["Compo_Zscore"] = 0.2 * (dfNew["EY_Zscore"] + dfNew["B/P_Zscore"] + dfNew["S/P_Zscore"] + dfNew["CFY_Zscore"] + dfNew["DY_Zscore"])
        dfNew = dfNew.dropna(subset=["Compo_Zscore"])
        dfNew = dfNew.sort_values(["Compo_Zscore"], ascending=False)
        return dfNew
    else:
        return None


# newList = ["ERUS", "GULF", "ITKY", "JKL", "RSX", "XTH"]
# newList = ['XOP','GXG','CEE']
newList = ["FYLD", "AVDV"]


def add_missing_unds(newList: list) -> None:
    with PADB_connection() as conn:
        dfExisting = pd.read_sql_query("SELECT * FROM ETF_RATIOS", conn)
        dfExisting["need_reimport"] = False  # need this for compatibility
    existingRatiosUnds = sorted(dfExisting["index"].to_list())
    undstoAdd = [c for c in newList if c not in existingRatiosUnds]
    if len(undstoAdd) > 0:
        logger.info(f"These are the {len(undstoAdd)} ETFs to add: {undstoAdd}")
        ratios, errs = selenium_scrap_ratios(undstoAdd, verbose=isLocal())
        dfNew = _prep_ratios(ratios, dfExisting)
        databases_update(dfNew, "ETF_RATIOS", idx=False, mode="replace", verbose=True, save_insqlite=True)
    else:
        logger.info("No ETFs to add")


def _refresh_existing_unds() -> tuple[list | None, pd.DataFrame | None]:
    with PADB_connection() as conn:
        dfExisting = pd.read_sql_query("SELECT * FROM ETF_RATIOS", conn)
    if dfExisting is None or len(dfExisting) == 0:
        logger.error("Big problem, ETF_RATIOS is empty! Will regenerate from saved list in Nov 2023")
        unds = regenerate_ETF_list()
        return unds, None

    else:
        dfExisting["need_reimport"] = dfExisting["Last_updated"].apply(need_reimport)
        undsUptodate = dfExisting[dfExisting["need_reimport"] == False]["index"].tolist()
        undsToUpdate = dfExisting[dfExisting["need_reimport"]]["index"].tolist()
        l = len(undsUptodate)
        if l > 0:
            logger.info(f"{l} underlyings already updated: {undsUptodate}")

        l = len(undsToUpdate)
        if l == 0:
            logger.success("No underlyings to update")
            return None, dfExisting
        elif l <= MAX_TOUPDATE:
            logger.info(f"{l} underlyings to update: {undsToUpdate}")
            unds = undsToUpdate
        else:
            logger.info(f"{len(undsToUpdate)} underlyings to update. Choosing {MAX_TOUPDATE} underlyings starting with the oldest ones")
            dfOld = dfExisting[dfExisting["index"].isin(undsToUpdate)].sort_values("Last_updated", ascending=True)
            if len(dfOld) > MAX_TOUPDATE:
                dfOld = dfOld.iloc[:MAX_TOUPDATE]
            unds = list(set(dfOld["index"].tolist()))
            logger.info(f"Scrapping {len(unds)} underlyings: {unds}")
        return unds, dfExisting


def update_secs() -> tuple[pd.DataFrame | None, list[str], pd.DataFrame | None]:
    undsToRefresh, dfExisting = _refresh_existing_unds()
    if undsToRefresh:
        ratios, errs = selenium_scrap_ratios(undsToRefresh, verbose=isLocal())
        dfNew = _prep_ratios(ratios, dfExisting)
    else:
        dfNew = None
    return dfNew, errs, ratios


def check_underlyings() -> list[str]:
    with PADB_connection() as conn:
        dfExisting = pd.read_sql_query("SELECT * FROM ETF_RATIOS", conn)
    res = dfExisting["index"].tolist()
    return sorted(res)


@timer
def ETFratios_toDB(verbose=True) -> tuple[str, pd.DataFrame | None]:
    dfFull, errs, dfNew = update_secs()
    if dfNew is not None and len(dfNew) > 0:
        databases_update(dfFull, "ETF_RATIOS", idx=False, mode="replace", verbose=verbose, save_insqlite=True)
        msg = f"Updated {len(dfNew)} underlyings"
        msg += f"\nErrors with {len(errs)} underlyings: {','.join(errs)}"
    else:
        msg = "No ETF data was returned by the scrapping process"
        logger.info(msg)
        return None
    return msg, dfNew


def ETFRATIOS_last_date():
    return SQLA_last_date("ETF_RATIOS")


ScrapRatios = Scrap("ETF_RATIOS", ETFratios_toDB, ETFRATIOS_last_date, datetoCompare=datetime.date.today().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    # add_missing_unds(newList=newList)

    # undsToRefresh = ["SPY", "GDX"]
    # ratios, errs = selenium_scrap_ratios(undsToRefresh, verbose=True)

    # exit(0)

    logger.info(f"Latest date in ETF_RATIOS: {ETFRATIOS_last_date()}")
    unds = check_underlyings()
    logger.info(f"Existing ({len(unds)}) underlyings: {unds}")
    # add_missing_unds(newList=newList)
    # exit(0)
    msg, res = ETFratios_toDB()
    if res is not None:
        logger.info(f"full DB: {res}")
    