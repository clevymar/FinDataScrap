import pandas as pd
from common import last_bd
import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)


from utils.utils import timer
from databases.database_mysql import SQLA_last_date, databases_update 
from databases.classes import Scrap

URL_ROOT = "http://www.worldgovernmentbonds.com/country/"
COUNTRIES=['united-states','germany','france','switzerland','united-kingdom',]

def maturity_string_to_nyears(s:str)->float:
    tenorType=s.split()[1]
    tenor=int(s.split()[0])
    if tenorType.startswith('year'):
        res=tenor
    elif tenorType.startswith('month'):
        res=tenor/12
    elif tenorType.startswith('week'):
        res=tenor/52    
    else:
        raise ValueError('Tenor unknown: '+s) 
    return res   


def get_curve(country): 
    DFs=pd.read_html(URL_ROOT+country+'/')
    df=DFs[0]
    df=df.iloc[:,[1,2]]
    df.columns=['Maturity','Yield']
    df.loc[:,'Yield']=df.loc[:,'Yield'].apply(lambda x: float(x.replace('%','')))
    return df

def scrap_govies(save_to_file=True):
    dfAll=pd.DataFrame()
    for country in COUNTRIES:
        print('Getting govies data for '+country)
        df=get_curve(country)
        df['country'] = country
        df['Date'] = last_bd
        if len(dfAll)==0:
            dfAll=df
        else:
            dfAll=pd.concat([dfAll,df])
    # if save_to_file:
    #     print('Saving to file: '+OUTPUT_FILE)
    #     dfAll.to_csv(OUTPUT_FILE, index=False)        
            
    return dfAll


@timer
def govies_toDB(verbose=False):
    df=scrap_govies()
    df['nYears']=df['Maturity'].apply(maturity_string_to_nyears)
    df=df[df['nYears']<=30]
    res=df[["Date","nYears","country","Yield"]]
    res.columns=['Date','nYears','Country','Rate']
    if verbose:
        print(res)
    databases_update(res,"GOVIES_TS",idx=False,mode='append',verbose=verbose, save_insqlite=True)
    return res

def import_govies(argument=None):
    msg=None
    try:
        res = govies_toDB()
        msg = f'Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
    except Exception as e:
        raise Exception('Error while downloading Govies') from e
    return msg

def govies_last_date():
    return SQLA_last_date("GOVIES_TS")

ScrapGovies = Scrap("GOVIES", govies_toDB, govies_last_date)

 
if __name__ == '__main__' :
    print(import_govies())