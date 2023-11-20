import datetime
import pandas as pd
import logging

import os
import sys
parentdir = os.path.dirname(os.path.abspath(__file__))
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from rich.console import Console
from rich.panel import Panel
from rich import box
console=Console()

from imports.import_govies import ScrapGovies
from imports.import_swaps import ScrapIRS
from imports.import_yahoo import ScrapYahoo
from imports.import_ETFRatios import ScrapRatios
from imports.import_technicals import ScrapTechnicals
from imports.import_commoCurves import ScrapCommosCurves
from imports.import_FXimpliedrates import ScrapFXImpliedRates
from imports.import_creditETF import ScrapCreditETF
from imports.import_CMEfuts import ScrapCMEFuts
from common import last_bd, need_reimport
from utils.utils import print_color, Color, isLocal, timer
from email_report import send_report, send_email





# log = logging.getLogger('logger')
# log.setLevel(logging.DEBUG)

# formatter = logging.Formatter('%(message)s - %(levelname)s - %(asctime)s ', datefmt='%Y-%m-%d %H:%M')

# fh = logging.FileHandler('import.log', encoding='utf-8')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# log.addHandler(fh)

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(formatter)
# log.addHandler(ch)


lstScrap = [ScrapGovies, ScrapIRS, ScrapYahoo, ScrapTechnicals, ScrapCommosCurves, ScrapFXImpliedRates, ScrapCreditETF, ScrapRatios, ScrapCMEFuts] 



def scrap_main(el):
    try:
        msg=""
        last_date = el.func_last_date()
        datetoCompare = el.datetoCompare
        need=need_reimport(last_date,datetoCompare)
        if need:
            print_color(f'\n\nFunc {el.func_scrap} will execute as latest date in DB was {last_date}','HEADER')
            try:
                res = el.func_scrap()
                if isinstance(res,list):
                    msg = f'Well downloaded for {el.name} - {len(res)} tables'
                    for item in res:
                        msg += f'\n\t {len(item)} rows, {len(item.columns)} cols'
                        print(msg)
                else:
                    if isinstance(res,pd.DataFrame):
                        msg = f'[+] Downloaded: {len(res)} rows, {len(res.columns)} cols for \033[6;30;42m{el.name}\033[0m'
                        print_color(msg, 'RESULT')
                    elif res is None:
                        msg = f'[-] No data downloaded for \033[6;30;42m{el.name}\033[0m'
                        print_color(msg, 'COMMENT')
                    elif isinstance(res,str):
                        msg=str
                    
            except Exception as e:
                msg=f'Error while scrapping with {el.func_scrap} for {el.name}'
                raise Exception(msg) from e
        else:
            msg = f"[i] - Data already scraped as of {last_date} - no need to reimport \033[6;30;42m{el.name}\033[0m"
            print_color(msg,'COMMENT')
        return msg
    except Exception as e:
        raise Exception(f'Error while scrapping for {el.name}') from e
    

@timer
def scrap_all():
    if not isLocal():
        print('\n\n\n','-'*30)
    msg=""
    for el in lstScrap:
        errorMessage = f'Error while scrapping for {el.name}'
        try:
            tmp = scrap_main(el)
            if tmp is None:
                tmp=errorMessage
            msg+=tmp+'\n'
        except Exception as e:
            print_color(errorMessage,'FAIL')
            msg+=errorMessage+'\n'
            print(e)

    print('\n\n')
    console.print(Panel(msg, title="Import Report",box=box.DOUBLE_EDGE,style="bold green"))
    send_email(subject='Daily financial scrapping' ,body=msg)

    if not isLocal():
        print('\n\n\n','*** IMPORT ENDED ***')


if __name__ == "__main__":
    scrap_all()
    send_report()
    


