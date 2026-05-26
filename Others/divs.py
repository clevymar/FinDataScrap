import yfinance as yf

# List of ETF tickers
etfs = ['SPY', 'VOO', 'VTI']

# Fetch data for each ETF
for etf in etfs:
    stock = yf.Ticker(etf)
    info = stock.info
    print(f"ETF: {etf}, Dividend Yield: {info.get('dividendYield')}")

