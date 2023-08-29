#%%
import json
import pandas as pd
import numpy as np
import datetime

import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from common import fichierTSUnderlyings, need_reimport, isLocal, last_bd
from utils.utils import timer, print_color
from scrap_selenium import selenium_scrap_ratios
from databases.database_mysql import SQLA_last_date, databases_update, PADB_connection,SQLA_read_table
from databases.classes import Scrap


#%%
def list_ETF_status()->pd.DataFrame:
    dictInput = json.load(open(fichierTSUnderlyings, "r"))
    undsTS = dictInput["EQTY_SPOTS"]

    dfStatic = SQLA_read_table('ETF_DB')
    undsStatic = dfStatic['ETF'].tolist()

    dfRatios = SQLA_read_table('ETF_RATIOS')
    undsRatios = dfRatios['index'].tolist()

    allETFs = list(set(undsTS + undsStatic + undsRatios))
    print(f"Total ETFs: {len(allETFs)}")

    tab=[]
    for etf in allETFs:
        tmp = { 'ETF':etf,
            'inStatic': etf in undsStatic,
            'inRatios': etf in undsRatios,
            'inTS': etf in undsTS,
        }
        tab.append(tmp)
        
    df=pd.DataFrame(tab)
    return df


def create_etf_ref_table(df:pd.DataFrame)->pd.DataFrame:
    special_etfs = {'EXX1':['xetr','EXX1.DE'],
                    'WNRG':['xlon','WNRG.AS'],
                    '2828':['xhkg','2828.HK'],
                    'EMVL':['xlon','EMVL.L'],
                    'ITKY':['xams','ITKY.AS'],
                    # 'CE9':['xpar','CE9.PA'],
                    'MLXIX':['funds/xnas','MLXIX']
                    }

    etfs_nottoRatio=["TLT","IEF","LQD","HYG","BND","JNK","TIP","WIP", "EMLC","IHY","PICB", 
                    "BWX","BWZ", "SHV","SHY","UDN", "VCIT", "VCLT","VCSH",
                    "DBC","DBA","DBB","GLD","SLV","USO",'FXA', 'FXB','FXC','FXE','FXF','FXY','CYB','JKL','GULF','XTH']

    df['specialRatio']=False
    df['DoNotRatio']=False


    for etf in special_etfs.keys():
        df.loc[df['ETF']==etf, 'specialRatio']="_".join(special_etfs[etf])

    for etf in etfs_nottoRatio:
        df.loc[df['ETF']==etf, 'DoNotRatio']=True    
    
    return df

def save_etf_ref_table(df:pd.DataFrame):
    databases_update(df,'ETF_REF', mode='replace', idx=False,verbose=True, save_insqlite=False)
    

df=list_ETF_status()
print('Current status of ETFs:\n',df)
df=create_etf_ref_table(df)
print('With added special cases:\n',df)

savetoDB = input('Do you want to overwrite the ETF_REF table? (y/n)')
if savetoDB.upper()=='Y':
    save_etf_ref_table(df)