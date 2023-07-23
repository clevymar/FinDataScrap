import datetime
import pandas as pd

from scrap_govies import ScrapGovies

end = datetime.date.today()-pd.tseries.offsets.BDay(1)+pd.tseries.offsets.Hour(23)+pd.tseries.offsets.Minute(59)
last_bd=end.strftime("%Y-%m-%d")

lstScrap = [ScrapGovies]

def need_reimport(last_in_DB:str):
    if last_in_DB=='None' or last_in_DB is None:
        need=True
    else:
        latest=datetime.datetime.strptime(last_in_DB,"%Y-%m-%d")
        # latest=latest+pd.tseries.offsets.Day(1-type_date)
        need=latest<datetime.datetime.strptime(last_bd,"%Y-%m-%d")
    return need



def scrap_main(el):
    last_date = el.func_last_date()
    print(last_date)
    need=need_reimport(last_date)
    if need:
        print(f'Func {el.func_scrap} should execute')
    else:
        print(f"Data for {el.name} already scraped as of {last_date} - no need to reimport")


for el in lstScrap:
    scrap_main(el)



