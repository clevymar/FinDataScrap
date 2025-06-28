# %%
import pandas as pd
import time
import traceback
from io import StringIO
from loguru import logger

from openbb import obb

obb.user.preferences.output_type = "dataframe"
from pandas.tseries.offsets import BDay
from datetime import datetime, date
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay


# %%
def prev_bus_day() -> date:
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    prev_business_day = (datetime.today() - us_bd).date()
    return prev_business_day


# %%
def get_futures_curve(symbol: str) -> pd.DataFrame:
    try:
        data = obb.derivatives.futures.curve(symbol=symbol, date=prev_bus_day())
    except Exception as e:
        logger.error(f"Error fetching futures curve with OBB for {symbol}: {e}")
        return pd.DataFrame()
    dfSettlement = pd.DataFrame()
    dfSettlement["Expiry"] = pd.to_datetime(data["expiration"]).dt.strftime("%b %Y").str.upper()
    dfSettlement["Last"] = data["price"].round(2)
    dfSettlement["Settle"] = data["price"].round(2)

    return dfSettlement.set_index("Expiry")


# %%
if __name__ == "__main__":
    print(get_futures_curve("KC", prev_bus_day()))
