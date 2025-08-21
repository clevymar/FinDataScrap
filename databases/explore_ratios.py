#%%

import os
from datetime import timedelta as td

import pandas as pd

from database_mysql import SQLA_read_table
from common import DIR_FILES

# %%

ETF_ratios = SQLA_read_table("ETF_RATIOS").set_index("index")
ETF_ratios.to_csv('ratios.csv')
# %%
