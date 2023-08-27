import pandas as pd
from pathlib import Path 
import os, sys



from utils.email_CLM import send_email, nice_table,send_cyril_andrea


def sub_TS(table, unds=""):
    res=sub_data1DB(table)  #includes filling for na
    if unds!="": res=res[unds]
    if table=="EQTY_SPOTS":
        perf=REPORT_performance(res,"EQUITY")
        perf.drop([],axis=1,inplace=True)
    else:
        perf=REPORT_performance(res,"FX")
    #perf.columns=['Asset','Last','1d return','5d return','YtD']
    # if table=="FX_SPOTS":
    #     perf['Asset']=perf['Asset'].apply(lambda s:"USD"+s)
    perf.set_index('Asset',inplace=True)
    for col in ['YtD', '1y return' ,'vs200dma','vs 52w low','vs 52w high']:
        perf[col]=perf[col].apply(lambda x:str(int(x))+'%' if (x==x) else '')
    perf["5d return"]=perf["5d return"].apply(lambda x:f"{x/100:.1%}")
    perf["1d return"]=perf["1d return"].apply(lambda x:f"{x/100:.1%}")
    return perf

def IRS_report():
    def sub_pivot(bd):
        IRStable=res[res["Date"]==bd]
        IRStable=IRStable[IRStable["Tenor"].isin(tenor_list)]
        IRStable['Tenor']=IRStable['Tenor'].apply(lambda s:'0'+s if len(s)==2 else s)
        pivot=pd.pivot_table(data=IRStable,values='Rate',index='Tenor',columns='CCY')
        pivot=pivot.round(2)
        return pivot

    tenor_list=['2Y','10Y','20Y']
    res=sub_data1DB("IRS_TS")
    res.reset_index(inplace=True)
    dates=res["Date"].unique()
    dates.sort()
    last_bd=dates[-1]
    prev_bd=dates[-2]
    week_bd=dates[-6]
    IRSlast=sub_pivot(last_bd)
    IRSprev=sub_pivot(prev_bd)
    IRSweek=sub_pivot(week_bd)
    IRSdiff1=(IRSlast-IRSprev).round(2)
    IRSdiff1=IRSdiff1.applymap(lambda x:f"{x:.2f}").applymap(lambda s:" "*(5-len(s))+s)
    
    IRSdiff2=(IRSlast-IRSweek).round(2)
    IRSdiff2=IRSdiff2.applymap(lambda x:f"{x:.2f}").applymap(lambda s:" "*(5-len(s))+s)
    
    IRSdiff=IRSdiff1+" | "+IRSdiff2
    
    return IRSlast,IRSdiff


def govies_report():
    def sub_pivot(bd):
        IRStable=res[res["Date"]==bd]
        IRStable=IRStable[IRStable["nYears"].isin(tenor_list)]
        pivot=pd.pivot_table(data=IRStable,values='Rate',index='nYears',columns='Country')
        pivot=pivot.round(2)
        return pivot

    tenor_list=[2.0,5.0,10.0,30.0]
    res=sub_data1DB("GOVIES_TS")
    res.reset_index(inplace=True)
    dates=res["Date"].unique()
    dates.sort()
    last_bd=dates[-1]
    goviesLast=sub_pivot(last_bd)
    
    # prev_bd=dates[-2]
    # week_bd=dates[-6]
    # IRSprev=sub_pivot(prev_bd)
    # IRSweek=sub_pivot(week_bd)
    # IRSdiff1=(IRSlast-IRSprev).round(2)
    # IRSdiff1=IRSdiff1.applymap(lambda x:f"{x:.2f}").applymap(lambda s:" "*(5-len(s))+s)
    
    # IRSdiff2=(IRSlast-IRSweek).round(2)
    # IRSdiff2=IRSdiff2.applymap(lambda x:f"{x:.2f}").applymap(lambda s:" "*(5-len(s))+s)
    
    # IRSdiff=IRSdiff1+" | "+IRSdiff2
    goviesDiff=None
    
    return goviesLast,goviesDiff




htm1,htm2,df1,df2=technicals.show_result_email()
technical_last=clm_config.load_setting('technicals_last')

"""                 prepares all the equity underlyings """
unds=[]
for famille in list_colls:
    unds+=common_modules.assets_coll[famille]
unds=list(set(unds))

"""                 loads ratios and spot data from DBs """
cnx=create_connection()
ETF_ratios=pd.read_sql_query("SELECT * FROM " +"ETF_RATIOS" , cnx, index_col="index")
cols=["Compo_Zscore","P/E1","P/B","P/S","P/CF","DY","Name"]
ratios=ETF_ratios[cols]
ratios=ratios.replace(["Select Sector SPDR® Fund","iShares ","MSCI ","SPDR® ","SPDR ","ETF", "Vanguard ","WisdomTree "],"",regex=True)
newcols=['zscore']+ratios.columns.tolist()[1:]
ratios.columns=newcols

perf_eq=sub_TS("EQTY_SPOTS")
perf_ccy=sub_TS("FX_SPOTS")
perf_commos=sub_TS("COMMOS_SPOTS")

"""                 concatenates the EQUITY tables """
df=pd.concat([ratios,perf_eq],axis=1)
df.sort_index(inplace=True)
cols=perf_eq.columns.to_list()
cols=cols+["Name"]
perf_eq=df[cols]
df.drop(['Last'],axis=1,inplace=True)
""" creates the daily csv used in XL Repartition files for valos """
dfxl=df.copy()
dfxl.drop(['1d return','5d return'],axis=1,inplace=True)
dfxl.dropna(axis=0,subset=['P/B'],inplace=True)
try:
    dfxl.to_csv(os.environ['ONEDRIVECONSUMER'] + "\\#Money_top\\ETF_RATIOS.csv")
    dfxl.to_csv(common_modules.dir_output+"\\ETF_RATIOS.csv")
except Exception as e:
    print('Error saving file ETF_RATIOS.csv')
    print(e)
""" now only keep relevant unds for the report in the dataframes """
df=df[df.index.isin(unds)]
df.index.rename('Asset',inplace=True)
perf_eq=perf_eq[perf_eq.index.isin(unds)]
ratios=ratios.loc[ratios.index.intersection(unds)]
""" back to report """

perf_ccy["Name"]=""
perf_commos["Name"]=""

"""                 now creates the html """

res=f'<h2>Technical signals, last imported on {technical_last}</h2><br> '
res+=nice_table(df1,min_chars=10)

lastDate,lastSpot,twentydma,hvol,width,band=technicals.SPXfut()
res+=f'<h3>Trading SPX signals</h3>'
res+=f'last ES spot {lastSpot}\t 20dMA {twentydma}\t 20d vol {hvol:,.1%} \n\t low band {band[0]}, high band {band[1]}<br><br>' 

"""                     IRS   & Govies              """
IRSlast,IRSdiff=IRS_report()
res+=f'<h2>IRS rates and changes</h2>'
res+=nice_table(IRSlast,min_chars=12,title="Latest swap rates",digits=2)
res+=nice_table(IRSdiff,min_chars=12,title="1d|5d changes",digits=2)

goviesLast,goviesDiff = govies_report()
res+=f'<h2>Govies rates and changes</h2>'
res+=nice_table(goviesLast,min_chars=12,title="Latest Govies rates",digits=2)
# res+=nice_table(IRSdiff,min_chars=12,title="1d|5d changes",digits=2)




dfall=pd.concat([perf_eq,perf_ccy,perf_commos],axis=0)
dfall.index.rename('Asset',inplace=True)
dfall2=dfall.copy()
dfall.dropna(subset=['1d return'],inplace=True)
dfall["1d return"]=dfall["1d return"].apply(lambda x:float(str(x)[:-1]))
dfall.sort_values(by='1d return',inplace=True)
dfall["1d return"]=dfall["1d return"].apply(lambda x:str(x)+'%')
res+=f'<h2>Biggest 1 days movers</h2>'
res+=nice_table(dfall.head(5),min_chars=12,title="Losers")
res+=nice_table(dfall.tail(5),min_chars=12,title="Winners")

dfall=dfall2.copy()
dfall.dropna(subset=['5d return'],inplace=True)
dfall["5d return"]=dfall["5d return"].apply(lambda x:float(str(x)[:-1]))
dfall.sort_values(by='5d return',inplace=True)
dfall["5d return"]=dfall["5d return"].apply(lambda x:str(x)+'%')
res+=f'<h2>Biggest 5 days movers</h2>'
res+=nice_table(dfall.head(5),min_chars=12,title="Losers")
res+=nice_table(dfall.tail(5),min_chars=12,title="Winners")

res+=f'<br><br>'+'<h2>FX and commos</h2>'
for dfl in [perf_ccy,perf_commos]:
    dfl.drop(['Name'],axis=1,inplace=True)
    dfl.sort_values('5d return',inplace=True)
res+=nice_table(perf_ccy,min_chars=10,title="FX- USD vs each currency")
res+='<br>'+nice_table(perf_commos,min_chars=10,title="Commodities")


res+='<br><br>'+'<h2>Valuation ratios and spot moves for major underlyings</h2>'
for famille in list_colls:
    etfs=common_modules.assets_coll[famille]
    extract=df.loc[etfs,:]
    extract.sort_values(by='zscore',inplace=True)
    extract.index.rename('Asset',inplace=True)
    tab=nice_table(extract,min_chars=6,title=famille)
    res+=tab+'<br>'

df.dropna(subset=['zscore'],inplace=True)
df.sort_values(by='zscore',inplace=True)
res+=f'<h2>Cheapest and most expensive</h2><br> '
res+=nice_table(df.head(5),min_chars=6,title="Dearest")
res+=nice_table(df.tail(5),min_chars=6,title="Cheapest")



# send_email(f"Daily quick mkts report", res)
#send_andrea(f"Daily quick mkts report", res)
send_cyril_andrea(f"Daily quick mkts report", res)