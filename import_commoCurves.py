#%%
import time
import numpy as np
import pandas as pd
from io import StringIO
import requests

from bs4 import BeautifulSoup

from scrap_selenium import selenium_scrap_simple
from credentials import QUANDL_KEY

# import quandl
# quandl.ApiConfig.api_key = QUANDL_KEY

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


        
#%%        

def download_from_file():
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
# Fund Ticker	Security Identifier	Identifier	Shares	Weight	Name	Contract Expiry Date	Sector	$ Value	Date
# Name,Security,Expiry,ID,NOSH,Value,%NAV,Exchange



df = download_from_file()

#%%
def clean_downloaded_data(df):
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
    return df3

df3 = clean_downloaded_data(df)
df3

#%%        
        
def scrap_compo_selenium():
    
    html_source = selenium_scrap_simple(URL_COMPO)
    source_data = html_source.encode('utf-8')
    soup = BeautifulSoup(source_data, "lxml")

    tbs=soup.findAll("tr", {"class": "ng-scope"})
    res=[]
    for tb in tbs:
        tds=tb.findAll("td")
        tab=[]
        for i in range(1,len(tds)):
            tab.append(tds[i].text)
        if tab[0]!="" and tab[4]!='0':
            res.append(tab)

    df=pd.DataFrame(res)
    print(df)
    df.columns=["Name","Security","Expiry","ID","NOSH","Value","%NAV"]
    df["Value"]=df["Value"].str.replace(',','').astype('float').round(0)
    try:
        df["%NAV"]=df["%NAV"].astype('float')
    except:
        """  %NAV empty, hence compute it manually """
        tot=df["Value"].sum()
        df["%NAV"]=(df["Value"]/tot).round(4)
    df["Exchange"]=df["Security"].apply(lambda s:s.split(' ', 1)[0])
    df.set_index("Name",inplace=True)
    print(df)


def import_single_cdty(nom,ticker,exchange,months):
    qroot="CHRIS/"
    qmkt=exchange+"_"
    qprod=ticker
    qmonths=months

    start=time.perf_counter()
    ric=qroot+qmkt+qprod+"1"
    print(f"QUANDL code : {ric[:-1]}")
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
    try:
        dfs['Roll1-2']=dfs[2]/dfs[1]-1
    except Exception as e:
        dfs['Roll1-2']=np.NaN
        print(f'[-] Error with {nom} : ',e)

    try:
        dfs['Roll2-3']=dfs[3]/dfs[2]-1
    except Exception as e:
        dfs['Roll2-3']=np.NaN
        print(f'[-] Error with {nom} : ',e)

    try:
        dfs['Roll1-6']=(dfs[6]/dfs[1]-1)/6
    except Exception as e:
        dfs['Roll1-6']=np.NaN
        print(f'[-] Error with {nom} : ',e)

    time_to_run=time.perf_counter()-start
    print(f"Time it took to run for {nom} : {time_to_run:.1f}s")
    
    return dfs


def import_all():
    first=True
    DBCweights=pd.read_csv(dir_main+DBCcompo,index_col=0)
    print("*** DBC latest weights ***")
    print(DBCweights)
    print("\n"*2)
    for index, row in cmdties.iterrows():
        nom=index
        ticker=row['Future']
        exch=row['Quandl_EXC']
        months=row['NB contracts']
        print("\n Importing "+nom)
        dft=import_single_cdty(nom,ticker,exch,months)
        try:
            res=DBCweights.loc[nom,"%NAV"]
        except:
            res=0
        dft["DBC Weight"]=res
        if first:
            df=dft
            first=False
        else:
            df=pd.concat([df,dft])
    """move the columns """
    cols=['Roll1-2','Roll2-3','Roll1-6']+list(range(1,25))+['Timestamp']+["DBC Weight"]
    df=df[cols]
    for c in df.columns[:3]:
        df[c]=df[c].apply(lambda x:round(100*x,1))
    print(df)
    df.to_csv(dir_main+"DBC_results.csv")
    clm_config.write_setting('commos_last')
    return df


def clm_html_table(df,red=False,show=False,nb_cols_title=1):
    vals=df.reset_index()
    if red:
        font_color=[['red' if type(v) != str and v<=0 else 'black' for v in vals[col].tolist()] for col in vals.columns[1:]]
        font_color[0]=['blue' for v in range(len(font_color[0]))]
    else:
        font_color='black'
    colw=20
    nbc=(len(vals.columns[1:])-nb_cols_title)
    colwidth=[4*colw]*nb_cols_title+[colw]*nbc
    fig = go.Figure(data=[go.Table(
                                    header=dict(values=list(vals.columns[1:]),
                                                fill_color='royalblue',
                                                font=dict(color='white', size=12),
                                                height=40,
                                                align=['left']*nb_cols_title+['right']*nbc),
                                    cells=dict(values=vals.T.values.tolist()[1:],
                                                fill_color='lightgrey',
                                                font = dict(color=font_color),
                                                align=['left']*nb_cols_title+['right']*nbc),
                                    columnwidth=colwidth )
                     ])
    if show:
        fig.show(renderer='browser')
    return fig




def present_results(df,load_from_file=False):
    if load_from_file:
        df=pd.read_csv(dir_main+"DBC_results.csv",index_col=0)
    dft=df.reset_index()
    cols=dft.columns.to_list()
    newcols=cols[:5]+["Category"]+[cols[-1]]
    dft["Category"]=dft[dft.columns[0]].apply(lambda s:cmdties.loc[s,"Category"])
    dft=dft[newcols]
    dft.columns=["Commodity"]+newcols[1:4]+['First fut']+newcols[-2:]
    dft['First fut']=dft['First fut'].apply(lambda s:f"{s:,.1f}".rstrip('0').rstrip('.'))  
    ts=df['Timestamp'].min()
    #print(f"\n Data for : {ts} \n")
    dfNonMissing=dft[~dft.iloc[:,1].isna()]
    dfNonMissing['DBC Weights rec']=dfNonMissing['DBC Weight']/dfNonMissing['DBC Weight'].sum()*100
    missingNb=dft.iloc[:,1].isna().sum()
    missingCommos=dft[dft.iloc[:,1].isna()].iloc[:,0].to_list()
    missingWeights=dft[dft.iloc[:,1].isna()].iloc[:,-1].sum().round(1)
    message=f'{missingNb} commodities witout roll data : {missingCommos} accounting for {missingWeights}% of DBC composition'
    dfc=dft[dft.columns[1:4]].fillna(0)
    s=dft[dft.columns[-1]].T
    res=s.dot(dfc)/100
    dft=dft[["Commodity","Category"]+dft.columns[1:-2].to_list()+[dft.columns[-1]]]
    fig=clm_html_table(dft,True,False,2)
    df_group=dft.groupby(by='Category').sum()["DBC Weight"]
    return fig, ts, res, df_group, message

def commos_for_dashboard(refresh_carry=False,update_DBC=False):
    if update_DBC: scrap_compo_selenium()
    if refresh_carry : import_all()
    fig, ts, res, df_group, message = present_results(None, True)
    return fig, ts, res, df_group, message


if __name__ == "__main__":
    download_from_file()
    # scrap_compo_selenium()
    #import_all()
    #present_results(None,True)
    # commos_for_dashboard(refresh_carry=False,update_DBC=False)