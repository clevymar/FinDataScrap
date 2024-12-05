#%%
""" copying the result of the ETF ratios scarpping to LOCAL file"""


import os

import pandas as pd
from rich.console import Console
console = Console()

from email_report import ETF_ratios_report, sub_TS

#%%
console.log("Getting ratios")
dfRatios = ETF_ratios_report()
console.log("Getting time series")
perf_eq = sub_TS("EQTY_SPOTS")

#%%

console.log("Preparing Equity data")
dfEquity = pd.concat([dfRatios, perf_eq], axis=1)
dfEquity.sort_index(inplace=True)
dfEquity=dfEquity.drop(["Last","1d return", "5d return"], axis=1).dropna(subset=['zscore'])

# creates the daily csv used in XL Repartition files for valos
dfEquity
# saves
console.log("Saving Equity data")
dfEquity.to_csv(os.environ["ONEDRIVECONSUMER"] + "\\#Money_top\\ETF_RATIOS.csv")
console.log(f"{len(dfEquity)} assets saved to {os.environ['ONEDRIVECONSUMER']}\\#Money_top\\ETF_RATIOS.csv")


