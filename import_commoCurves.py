#%%
import pandas as pd
from io import StringIO
import requests

from bs4 import BeautifulSoup

from scrap_selenium import selenium_scrap_simple
from credentials import QUANDL_KEY
from utils import print_color,timer
from classes import Scrap
from database_mysql import SQLA_last_date, databases_update,SQLA_read_table
from common import last_bd

import quandl
quandl.ApiConfig.api_key = QUANDL_KEY

pd.options.mode.chained_assignment = None

URL_COMPO = "https://www.invesco.com/us/financial-products/etfs/holdings?audienceType=Advisor&ticker=DBC"

DICT_NAMES = {
    'Brent Crude Oil':'Brent Crude',
    'CSC Number 11 World Sugar':'Sugar',
    'Gold 100 Troy Ounces':'Gold',
    'Henry Hub Natural Gas':'Natural Gas',
    'Light Sweet Crude Oil':'WTI Crude',
    'Primary Aluminum':'Aluminium',
    'Reformulated Gasoline Blendstock for Oxygen Blending':'Gasoline',
    'Soybean':'Soybeans',
 }


def _download_from_file():
    csv_url = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Advisor&action=download&ticker=DBC"
    response = requests.get(csv_url)
    df=None
    if response.status_code == 200:
        # Create a file-like object from the response content
        content = response.content
        content_type = response.headers["Content-Type"]

        # Check if the content is CSV
        if "text/csv" in content_type:
            df = pd.read_csv(StringIO(content.decode('utf-8')))
        else:
            print("The URL does not point to a CSV file.")
    else:
        print("Failed to retrieve the CSV file from the URL.")
    
    if len(df)>0:
        df=df[df['Sector']!='Collateral']    
        df=df.iloc[:,1:]
    return df


def _clean_downloaded_data(df):
    df2 = df[['Name','Contract Expiry Date','Shares','$ Value','Weight']]
    df2.columns = ['Security','Expiry','NOSH','Value','%NAV']

    df2['Exch']=df2['Security'].apply(lambda s:s.split()[0])
    df2['Name']=df2['Security'].apply(lambda s:" ".join(s.split()[1:-1]))
    df3=df2.groupby(['Name','Security','Exch']).agg({
                                                        'Expiry':'first',
                                                        'NOSH':sum,'Value':sum,'%NAV':sum
                                                    })

    df3=df3.reset_index()
    df3['Exchange']=df3['Exch']
    df3.drop('Exch',axis=1,inplace=True)
    df3['Name']=df3['Name'].apply(lambda s:DICT_NAMES.get(s,s))
    df3['Date']=last_bd
    df3.set_index("Name",inplace=True)

    return df3


@timer
def saveCompo_toDB(verbose=True)->pd.DataFrame:
    """
    The function `saveCompo_toDB` downloads DBC compositions from a csv on the provider website, cleans it, prints the cleaned data if
    `verbose` is True, updates a database with the cleaned data, and returns the cleaned data.
    
    :return: the composition of DBC
    """
    
    df = _download_from_file()
    dfCompo = _clean_downloaded_data(df)
    if verbose:
        print_color(f'*** DBC latest weights ***',color='RESULT')
        print(dfCompo)
    databases_update(dfCompo.reset_index(),"COMPO_DBC",idx=False,mode='replace',verbose=verbose, save_insqlite=True)
    return dfCompo



def _import_single_cdty(nom,ticker,exchange,months):
    qroot="CHRIS/"
    qmkt=exchange+"_"
    qprod=ticker
    qmonths=months
    ric=qroot+qmkt+qprod+"1"
    # print(f"QUANDL code : {ric[:-1]}")
    df=quandl.get(ric,rows=1)
    df["TICKER"]=qprod+"1"
    df["MONTH"]=1
    lastdate=df.index[0]
    for i in range(2,qmonths-1):
        ric=qroot+qmkt+qprod+str(i)
        dftemp = quandl.get(ric,rows=1)
        if dftemp.index[0]>=lastdate:
            dftemp["TICKER"]=qprod+str(i)
            dftemp["MONTH"]=i
            df=pd.concat([df,dftemp])
    """ now transposes and produces relevant info """
    dfs=df.reset_index()[["Settle","MONTH"]]
    dfs['MONTH']=dfs['MONTH'].astype('int')
    dfs.set_index("MONTH",inplace=True)
    dfs=dfs.T 
    dfs.index=[nom]
    dfs['Timestamp']=lastdate
    
    for rollMats in [(1,2),(2,3),(1,6)]:
        start=rollMats[0]
        end=rollMats[1]
        try:
            dfs[f'Roll{start}-{end}']=dfs[end]/dfs[start]-1
        except Exception as e:
            dfs[f'Roll{start}-{end}']=float('nan')
            print_color(f'[-] Error computing rolls for {nom}','FAIL')
            print(e)
    
    return dfs


def import_commos_curves(verbose=True)->pd.DataFrame:
    """
    The function `import_commos_curves` imports commodity curves data from quandl for all futures in DBC and compute rolls.
    :return: full term structures, and a few rolls.
    """
    dfDBCweights=SQLA_read_table("COMPO_DBC")
    dfMaster = SQLA_read_table("COMMO_FUTURES_MASTER")
    dfCommos = pd.merge(dfMaster,dfDBCweights[["Name","%NAV"]],left_on="Name",right_on="Name",how="left")
    dfCommos.set_index("Name",inplace=True)
    if verbose: 
        print_color("DBC weights and futures to scrap:","COMMENT")
        print(dfCommos)
    
    tab=[]
    for index, row in dfCommos.iterrows():
        nom=index
        ticker=row['Future']
        exch=row['Quandl_EXC']
        months=row['NB contracts']
        weight=row['%NAV']
        print("\n Importing "+nom)
        dft=_import_single_cdty(nom,ticker,exch,months)
        dft["DBC Weight"]=weight
        tab.append(dft)
    
    df=pd.concat(tab)
    """move the columns """
    cols=['Roll1-2','Roll2-3','Roll1-6']+list(range(1,25))+['Timestamp']+["DBC Weight"]
    df=df[cols]
    for c in df.columns[:3]:
        df[c]=df[c].apply(lambda x:round(100*x,1))
    df['Date']=last_bd
    return df


@timer
def saveCommoCurves_toDB(verbose=True):
    df = import_commos_curves(verbose=verbose)
    if verbose:
        print_color(f'*** Commos future curves ***',color='RESULT')
        print(df)
    databases_update(df,"COMMO_FUTURES_CURVES",idx=False,mode='replace',verbose=verbose, save_insqlite=True)
    return df


def save_all_commos_toDB(verbose=True):
    saveCompo_toDB(verbose=verbose)
    saveCommoCurves_toDB(verbose=verbose)

def import_commosCurves(verbose=True)->str:
    """
    The function `import_commosCurves` downloads DBC composition data and commodity futures curves data from the web,
    saves the data into a database, 
    and returns a message indicating the success or failure of the download.
    
    :return: a message string.
    """
    msg=None
    try:
        res = saveCompo_toDB(verbose=verbose)
        msg = f'DBC composition well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
    except Exception as e:
        msg='Error while downloading DBC compo'
        print_color(msg,'FAIL')
        print(e)
    
    try:
        res = saveCommoCurves_toDB(verbose=verbose)
        msg += f'Commos curves well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
    except Exception as e:
        msg+='Error while commos future curves'
        print_color('Error while commos future curves','FAIL')
        raise e
    
    return msg

def commosCurves_last_date():
    return SQLA_last_date("COMPO_DBC")




ScrapCommosCurves = Scrap("COMMOS CURVES", save_all_commos_toDB, commosCurves_last_date)





if __name__ == "__main__":
    # import_commosCurves()
    # scrap_compo_selenium()
    #import_all()
    #present_results(None,True)
    # commos_for_dashboard(refresh_carry=False,update_DBC=False)
    import_commosCurves()