#%%
import pandas as pd
from pathlib import Path 
import os, sys
# print(os.getcwd())

# os.chdir(r'c:\Users\clevy\OneDrive\Python Scripts\Finance\FinDashboard\ImportData\GCP')

from databases.database_mysql import SQLA_read_table

# %%


def sub_data1DB(tablename: str):
    res = SQLA_read_table(tablename)
    if "Date" in res.columns:
        res.set_index("Date", inplace=True)
        res.index = res.index.map(lambda s: str(s)[:10])
        res.fillna(method="ffill", inplace=True)
    return res




def sub_pivot(bd):
        IRStable = res[res["Date"] == bd]
        IRStable = IRStable[IRStable["Tenor"].isin(tenor_list)]
        IRStable["Tenor"] = IRStable["Tenor"].apply(lambda s: "0" + s if len(s) == 2 else s)
        pivot = pd.pivot_table(data=IRStable, values="Rate", index="Tenor", columns="CCY")
        pivot = pivot.round(2)
        return pivot

tenor_list = ["2Y", "10Y", "20Y"]
res = sub_data1DB("IRS_TS")
res.reset_index(inplace=True)
dates = res["Date"].unique()
dates.sort()
last_bd = dates[-1]
prev_bd = dates[-2]
week_bd = dates[-6]
IRSlast = sub_pivot(last_bd)
IRSprev = sub_pivot(prev_bd)
IRSweek = sub_pivot(week_bd)
IRSdiff1 = (IRSlast - IRSprev).round(2)
IRSdiff1 = IRSdiff1.applymap(lambda x: f"{x:.2f}").applymap(lambda s: " " * (5 - len(s)) + s)

IRSdiff2 = (IRSlast - IRSweek).round(2)
IRSdiff2 = IRSdiff2.applymap(lambda x: f"{x:.2f}").applymap(lambda s: " " * (5 - len(s)) + s)

IRSdiff = IRSdiff1 + " | " + IRSdiff2

#%%
res

#%%
data=res[res['Tenor'].isin(tenor_list)]
pt = pd.pivot_table(data=data, values="Rate", index="Date", columns=["CCY",'Tenor'])
res = pt.ffill().stack().stack().reset_index().rename(columns={0:'Rate'})
res=res[['Date','CCY','Tenor','Rate']]
res
