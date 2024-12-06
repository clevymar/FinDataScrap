import os
import sys
from time import perf_counter
from typing import Any

parentdir = os.path.dirname(os.path.abspath(__file__))
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich import box
from loguru import logger

console = Console()

from imports.import_govies import ScrapGovies
from imports.import_swaps import ScrapIRS
from imports.import_yahoo import ScrapYahoo
from imports.import_ETFRatios import ScrapRatios
from imports.import_technicals import ScrapTechnicals
from imports.import_commoCurves import ScrapCommosCurves
from imports.import_FXimpliedrates import ScrapFXImpliedRates
from imports.import_creditETF import ScrapCreditETF
from imports.import_CMEfuts import ScrapCMEFuts
from imports.import_tips import ScrapTIPS

from common import need_reimport
from utils.utils import isLocal, timer
from email_report import send_report, send_email
from databases.classes import Scrap


lstScrap = [
    ScrapGovies,
    ScrapTIPS,
    ScrapIRS,
    ScrapYahoo,
    ScrapTechnicals,
    ScrapFXImpliedRates,
    ScrapCreditETF,
    ScrapRatios,
    ScrapCMEFuts,  #leave towards the end as it is the latest website updated
    ScrapCommosCurves,
]


def scrap_main(el: Scrap) -> str:
    def output_string(el: Scrap, start: str, type_: str):
        nicestr = f"{start} \033[6;30;42m{el.name}\033[0m"
        if type_ == "RESULT":
            logger.success(nicestr)
        elif type_ == "ERROR":
            logger.error(nicestr)
        else:
            logger.info(nicestr)
        return f"{start} {el.name}"

    def manage_results(el: Scrap) -> tuple[Any, str]:
        res = el.func_scrap()
        if isinstance(res, list):
            msg = f"[+] Well downloaded for {el.name} - {len(res)} tables"
            for item in res:
                msg += f"\n\t {len(item)} rows, {len(item.columns)} cols"
                logger.success(msg)
        elif isinstance(res, tuple):
            msg = f"Downloaded for {el.name}\n"
            msg += res[0]
            logger.info(msg)
        elif isinstance(res, pd.DataFrame):
            msg = output_string(el, f"[+] Downloaded: {len(res)} rows, {len(res.columns)} cols for ", "RESULT")
        elif isinstance(res, str):
            msg = f"Downloaded for {el.name}\n"
            msg += res
            logger.info(msg)
        elif res is None:
            msg = output_string(el, "[-] No data downloaded for ", "RESULT")
        return res, msg

    try:
        msg = ""
        last_date = el.func_last_date()
        datetoCompare = el.datetoCompare
        need = need_reimport(last_date, datetoCompare)
        if need:
            logger.info(f"\n\nFunc {el.func_scrap} will execute as latest date in DB was {last_date}")
            t0 = perf_counter()
            try:
                res, msg = manage_results(el)

            except Exception as e:
                msg = f"[-] Error while scrapping with {el.func_scrap} for {el.name}"
                logger.exception(msg)
                raise Exception(msg) from e
            finally:
                msg += f"\n\t - run in {perf_counter()-t0:.0f} seconds"
        else:
            msg = output_string(el, f"[i] Data already scraped as of {last_date} - no need to reimport ", "RESULT")

        return msg
    except Exception as e:
        raise Exception(f"Error while scrapping for {el.name}") from e


@timer
def scrap_all():
    if not isLocal():
        print("\n\n\n" + "-" * 30)
    msg = ""
    hasError = False
    for el in lstScrap:
        errorMessage = f"Error while scrapping for {el.name}"
        try:
            console.print('\n\n')
            console.rule(f"Scraping {el.name}")
            tmp = scrap_main(el)
            if tmp is None:
                tmp = errorMessage
                hasError = True
            msg += tmp + "\n"
        except Exception as e:
            logger.error("MAJOR - " + errorMessage)
            msg += errorMessage + "\n"
            hasError = True
            logger.exception(e)

    print("\n\n")
    console.print(Panel(msg, title="Import Report", box=box.DOUBLE_EDGE, style="bold green"))
    subject = "Daily financial scrapping"
    if hasError:
        subject += " - some Error while importing data"
    send_email(subject=subject, body=msg)

    if not isLocal():
        logger.success("\n\n\n" + "*** IMPORT ENDED ***")


if __name__ == "__main__":
    scrap_all()
    send_report()
