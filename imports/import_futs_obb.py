# %%
import pandas as pd
import tempfile
from loguru import logger

from openbb import obb
import yfinance as yf

obb.user.preferences.output_type = "dataframe"
from datetime import datetime, date
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay


# %%
def prev_bus_day() -> date:
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    prev_business_day = (datetime.today() - us_bd).date()
    return prev_business_day


SUGAR_CONTRACT_MONTHS = {
    3: ("H", "MAR"),
    5: ("K", "MAY"),
    7: ("N", "JUL"),
    10: ("V", "OCT"),
}


def _sugar_yahoo_contracts(max_contracts: int = 12) -> list[tuple[str, str]]:
    today = datetime.today().date()
    contracts = []
    for year in range(today.year, today.year + 5):
        for month, (code, month_label) in SUGAR_CONTRACT_MONTHS.items():
            if year == today.year and month <= today.month:
                continue
            ticker = f"SB{code}{str(year)[-2:]}.NYB"
            expiry = f"{month_label} {year}"
            contracts.append((ticker, expiry))
            if len(contracts) >= max_contracts:
                return contracts
    return contracts


def _get_sugar_curve_yahoo() -> pd.DataFrame:
    yf.set_tz_cache_location(tempfile.gettempdir())
    rows = []
    contracts = _sugar_yahoo_contracts()
    logger.info(f"Fetching Sugar curve from Yahoo explicit contracts: {[ticker for ticker, _ in contracts]}")

    for ticker, expiry in contracts:
        try:
            data = yf.download(
                ticker,
                period="5d",
                interval="1d",
                progress=False,
                auto_adjust=False,
                threads=False,
            )
        except Exception as e:
            logger.warning(f"Could not fetch Yahoo Sugar contract {ticker}: {e}")
            continue

        if data.empty or "Close" not in data:
            logger.warning(f"Yahoo returned no close prices for Sugar contract {ticker}")
            continue

        closes = data["Close"]
        if isinstance(closes, pd.DataFrame):
            closes = closes[ticker] if ticker in closes.columns else closes.iloc[:, 0]
        closes = closes.dropna()
        if closes.empty:
            logger.warning(f"Yahoo close prices were empty for Sugar contract {ticker}")
            continue

        price = round(float(closes.iloc[-1]), 2)
        rows.append([expiry, price, price])
        logger.debug(f"Sugar Yahoo contract {ticker}: expiry={expiry}, price={price}")

    if not rows:
        logger.error("Yahoo Sugar fallback returned no usable contracts")
        return pd.DataFrame()

    dfSettlement = pd.DataFrame(rows, columns=["Expiry", "Last", "Settle"])
    return dfSettlement.set_index("Expiry")


# %%
def get_futures_curve(symbol: str) -> pd.DataFrame:
    try:
        data = obb.derivatives.futures.curve(symbol=symbol, date=prev_bus_day())
    except Exception as e:
        logger.error(f"Error fetching futures curve with OBB for {symbol}: {e}")
        if symbol == "SB":
            logger.warning("Trying Yahoo explicit-contract fallback for Sugar (SB)")
            return _get_sugar_curve_yahoo()
        return pd.DataFrame()

    if data.empty:
        logger.warning(f"OBB returned an empty futures curve for {symbol}")
        if symbol == "SB":
            logger.warning("Trying Yahoo explicit-contract fallback for Sugar (SB)")
            return _get_sugar_curve_yahoo()
        return pd.DataFrame()

    dfSettlement = pd.DataFrame()
    dfSettlement["Expiry"] = pd.to_datetime(data["expiration"]).dt.strftime("%b %Y").str.upper()
    dfSettlement["Last"] = data["price"].round(2)
    dfSettlement["Settle"] = data["price"].round(2)

    return dfSettlement.set_index("Expiry")


# %%
if __name__ == "__main__":
    print(get_futures_curve("SB"))
