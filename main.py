import datetime
import pandas as pd
import logging

from import_govies import ScrapGovies
from import_swaps import ScrapIRS
from import_yahoo import ScrapYahoo
from import_ETFRatios import ScrapRatios
from import_technicals import ScrapTechnicals
from import_commoCurves import ScrapCommosCurves
from import_FXimpliedrates import ScrapFXImpliedRates
from common import last_bd, need_reimport
from utils import print_color, Color

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


lstScrap = [ScrapGovies, ScrapIRS, ScrapYahoo, ScrapTechnicals, ScrapCommosCurves, ScrapFXImpliedRates, ScrapRatios] # ScrapRatios   



def scrap_main(el):
    try:
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
                        msg = f'\t {len(item)} rows, {len(item.columns)} cols'
                        print(msg)
                else:
                    msg = f'[+] Downloaded: {len(res)} rows, {len(res.columns)} cols for \033[6;30;42m{el.name}'
                    print_color(msg, 'RESULT')
            except Exception as e:
                raise Exception(f'Error while scrapping with {el.func_scrap} for {el.name}') from e
        else:
            print_color(f"[i] - Data already scraped as of {last_date} - no need to reimport \033[6;30;42m{el.name}",'COMMENT')
    except Exception as e:
        raise Exception(f'Error while scrapping for {el.name}') from e


for el in lstScrap:
    scrap_main(el)



