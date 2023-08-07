import datetime
import pandas as pd
import logging

from import_govies import ScrapGovies
from import_swaps import ScrapIRS
from import_yahoo import ScrapYahoo
from common import last_bd

log = logging.getLogger('logger')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(message)s - %(levelname)s - %(asctime)s ', datefmt='%Y-%m-%d %H:%M')

fh = logging.FileHandler('import.log', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
log.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)


lstScrap = [ScrapGovies, ScrapIRS, ScrapYahoo]

def need_reimport(last_in_DB:str):
    if last_in_DB=='None' or last_in_DB is None:
        need=True
    else:
        if isinstance(last_in_DB, datetime.datetime):
            latest=last_in_DB
        else:
            latest=datetime.datetime.strptime(last_in_DB,"%Y-%m-%d")
        # latest=latest+pd.tseries.offsets.Day(1-type_date)
        need=latest<datetime.datetime.strptime(last_bd,"%Y-%m-%d")
    return need

def scrap_main(el):
    try:
        last_date = el.func_last_date()
        need=need_reimport(last_date)
        if need:
            print(f'Func {el.func_scrap} will execute as latest date in DB was {last_date}')
            try:
                res = el.func_scrap()
                if isinstance(res,list):
                    msg = f'Well downloaded for {el.name} - {len(res)} tables'
                    for item in res:
                        msg = f'\t {len(item)} rows, {len(item.columns)} cols'
                        print(msg)
                else:
                    msg = f'Well downloaded for {el.name} - {len(res)} rows, {len(res.columns)} cols'
                    print(msg)
            except Exception as e:
                raise Exception(f'Error while scrapping with {el.func_scrap} for {el.name}') from e
        else:
            print(f"Data for {el.name} already scraped as of {last_date} - no need to reimport")
    except Exception as e:
        raise Exception(f'Error while scrapping for {el.name}') from e


for el in lstScrap:
    scrap_main(el)



