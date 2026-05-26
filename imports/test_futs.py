#%%
# Using OpenBB to get the term structure (futures curve) for KC=F
import pandas as pd
from openbb import obb
obb.user.preferences.output_type = "dataframe"
from pandas.tseries.offsets import BDay
from datetime import datetime
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay


#%%
us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
prev_business_day = (datetime.today() - us_bd).date()
print(f"Previous business day (excluding US bank holidays): {prev_business_day}")


#%%

data = obb.derivatives.futures.curve(symbol="CL", date = prev_business_day)
data

#%%
# import plotly.express as px 
# px.line(data)