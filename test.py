#%%
import pandas as pd
from pathlib import Path 
import os, sys
# print(os.getcwd())

# os.chdir(r'c:\Users\clevy\OneDrive\Python Scripts\Finance\FinDashboard\ImportData\GCP')

from databases.database_mysql import SQLA_read_table

# %%

dfTech = SQLA_read_table("TECHNICALS")
print(dfTech)