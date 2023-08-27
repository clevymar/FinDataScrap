import json
import pandas as pd
from random import choices
import numpy as np
import datetime

from common import fichierTSUnderlyings, need_reimport, isLocal, last_bd
from utils import timer, print_color
from scrap_selenium import selenium_scrap_ratios
from database_mysql import SQLA_last_date, databases_update, PADB_connection
from classes import Scrap

COLS_MORNINGSTAR=["ETF","P/E1","P/B","P/S","P/CF","DY","EG","HG","SG","CFG","BG","Composite","Last_updated","UpdateMode","URL","Name"]
COLS_RATIOS=["EY","B/P","S/P","DY","Compo_Zscore"]
SECTORS =["Basic Materials","Consumer Cyclical","Financial Services", "Real Estate","Communication Services",
              "Energy","Industrials","Technology", "Consumer Defensive", "Healthcare", "Utilities"  ]

MAX_TOUPDATE = int(10 * 60 / 10) #* around 10 secs per udnerlying, want to limit to 10 mins
# MAX_TOUPDATE = 2

benchmark='ACWI'
dictInput = json.load(open(fichierTSUnderlyings, "r"))
secList = dictInput["EQTY_SPOTS"]
    
ratios=[]
row_dict={x:"" for x in COLS_MORNINGSTAR}
errs=[]
pd.set_option('mode.chained_assignment', None)

def _compute_extra_ratios(ratios:pd.DataFrame):
    if len(ratios)>0:
        tab=pd.DataFrame(ratios, columns=COLS_MORNINGSTAR+SECTORS)
        tab=tab.set_index("ETF")
        tab_ratios=tab[["P/E1","P/B","P/S","P/CF","DY","Composite","Last_updated","UpdateMode","URL","Name"]]
        inverter = lambda x: 1/x  
        pctshow = lambda x: 100*x
        for x in ["P/E1","P/B","P/S","P/CF"]:
            tab_ratios.loc[:,x]= tab_ratios.loc[:,x].map(inverter)
        for x in ["P/E1","P/CF"]:
            tab_ratios.loc[:,x]= tab_ratios.loc[:,x].map(pctshow)
            
        tab_ratios.columns=["EY","B/P","S/P","CFY","DY","Compo_Zscore","Last_updated","UpdateMode","URL","Name"]    
        for col in ["EY","B/P","S/P","CFY","DY"]: 
            col_z=col + "_Zscore"
            tab_ratios.loc[:,col_z]=np.nan
        tab_ratios.loc[:,"Compo_Zscore"] = np.nan
        #* create composite table """
        tab.drop(['DY',"Last_updated","UpdateMode","URL","Name"],axis=1,inplace=True)
        tabf=pd.concat([tab_ratios,tab],axis=1)
        return tabf
    
        
def update_secs():
    with PADB_connection() as conn:
        dfExisting = pd.read_sql_query("SELECT * FROM ETF_RATIOS", conn)
        dfExisting['need_reimport'] = dfExisting['Last_updated'].apply(need_reimport)
        undsUptodate = dfExisting[dfExisting['need_reimport'] == False]['index'].tolist()
        undsToUpdate = dfExisting[dfExisting['need_reimport']]['index'].tolist()
        
        l=len(undsUptodate)
        if l>0:
            print_color(f"{l} underlyings already updated","COMMENT")
            print('\t',undsUptodate)
            
        l = len(undsToUpdate)
        if l==0:
            print_color("No underlyings to update","RESULT")
            return
        elif l<=MAX_TOUPDATE:
            print_color(f"{l} underlyings to update","COMMENT")
            unds = undsToUpdate
            print('\t',unds)
        else:
            print_color(f"{len(undsToUpdate)} underlyings to update\nChoosing {MAX_TOUPDATE} underlyings starting with the oldest ones","COMMENT")
            dfOld = dfExisting[dfExisting['index'].isin(undsToUpdate)].sort_values('Last_updated',ascending=True)
            if len(dfOld)>MAX_TOUPDATE:
                dfOld = dfOld.iloc[:MAX_TOUPDATE]
            unds = dfOld['index'].tolist()
            print('\t',unds)
        
        ratios,errs = selenium_scrap_ratios(unds,verbose=isLocal())
        res=_compute_extra_ratios(ratios)
        if len(res)>0:
            res=res.reset_index().rename(columns={"ETF":'index'})
            res["Date"]=res['Last_updated']
            updatedUnds = res['index'].tolist()
            dfOld = dfExisting[~dfExisting['index'].isin(updatedUnds)].drop('need_reimport',axis=1)
            dfNew = pd.concat([dfOld,res],axis=0).reset_index(drop=True)
            #* now recreates the composite table 
            #TODO should I do it only when ACWI updated ?
            for col in ["EY","B/P","S/P","CFY","DY"]: 
                # dfNew[col] = dfNew[col].astype(float)
                col_z=col + "_Zscore"
                avg=dfNew.loc[dfNew['index']==benchmark,col].iloc[0]
                standev = dfNew[col].std()
                dfNew[col_z]=(dfNew[col]-avg) / standev
            dfNew["Compo_Zscore"] = 0.2*(dfNew["EY_Zscore"]  +dfNew["B/P_Zscore"]  +dfNew["S/P_Zscore"]  +dfNew["CFY_Zscore"] +dfNew["DY_Zscore"])  
            dfNew=dfNew.dropna(subset=["Compo_Zscore"])
            dfNew=dfNew.sort_values(["Compo_Zscore"], ascending=False)
            
        return dfNew


@timer
def ETFratios_toDB(verbose=True):
    dfNew = update_secs()
    databases_update(dfNew, "ETF_RATIOS",idx=False,mode='replace',verbose=verbose,save_insqlite=True)
    return dfNew
                                                                        
def ETFRATIOS_last_date():
    return SQLA_last_date("ETF_RATIOS")
        
ScrapRatios = Scrap("ETF_RATIOS", ETFratios_toDB, ETFRATIOS_last_date,datetoCompare=datetime.date.today().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    print(ETFRATIOS_last_date())
    res = ETFratios_toDB()
    print("full DB:\n",res) 
    print("updated:\n",res[res['Date']==last_bd])