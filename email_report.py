import os
from datetime import timedelta as td

import pandas as pd
from rich.console import Console
from rich.rule import Rule
# from icecream import ic

from utils.email_CLM import send_email, nice_table, send_cyril_andrea
from databases.database_mysql import SQLA_read_table
from common import DIR_FILES

console = Console()

# TODO create a shared config file, or probably table, for the underlyings collection
# * for the moment copied from common_modules

FX_list = ["EUR", "JPY", "CHF", "NOK", "SEK", "CAD", "BRL", "MXN", "GBP", "RUB", "TRY", "INR", "IDR", "ZAR", "THB", "AUD"]
basics = [
    "SPY",
    "QQQ",
    "EEM",
    "ERUS",
    "EFA",
    "FXI",
    "DBC",
    "GLD",
    "GDX",
    "TLT",
    "DBA",
    "DBB",
    "TUR",
    "GREK",
    "SLV",
    "XLE",
    "XLF",
    "XOP",
    "USO",
    "IYR",
    "EWZ",
    "EWJ",
    "DGS",
    "AAXJ",
    "IHY",
    "HYG",
    "LQD",
    "IEF",
    "PXH",
    "IWN",
    "IWM",
    "^STOXX50E",
    "^N225",
    "^HSCE",
    "FM",
    "VBR",
]
assets_coll = {
    "FX (USD vs XXX)": FX_list,
    "Main Asset Classes": ["ACWI", "SPY", "EFA", "EEM", "FM", "GLD", "DBC", "TLT", "HYG"],
    "Main indices": ["ACWI", "SPY", "QQQ", "EEM", "EFA", "EWJ", "^STOXX50E", "^N225", "^HSCE"],
    "Main Developped": ["SPY", "QQQ", "EFA", "IEUR", "EWJ", "EPP", "IDEV", "EWU"],
    "US equities": ["SPY", "QQQ", "IWM", "IWN", "VBR", "IWD", "IWF", "MGK"],
    "US sectors": ["SPY", "XBI", "XOP", "XLB", "XLE", "XLI", "XLK", "XLF", "XLP", "XLU", "XLV", "XLY", "XME", "XSD"],
    "Europe equities": ["IEUR", "IEV", "EZU", "EWG", "EWI", "EWP", "EWQ", "EWU"],
    "EM equities": ["EEM", "EMVL", "EEMV", "ECOW", "DEM", "PXH", "DGS", "FXI", "EWZ", "ERUS", "TUR", "GREK", "FM", "EZA", "NGE", "EYLD"],
    "Commodity equities": ["GNR", "XOP", "XLE", "USAI", "PICK", "XME", "GDX", "GDXJ", "URA", "SIL"],
    "Value": [
        "EMVL",
        "EEMV",
        "ECOW",
        "DEM",
        "IUSV",
        "VONV",
        "EFV",
        "FYLD",
        "FVAL",
        "SYLD",
        "IVAL",
        "GVAL",
        "QVAL",
        "PRF",
        "VLUE",
        "IVE",
        "VTV",
        "VBR",
        "EYLD",
        "PXH",
    ],
    "Study": ["IUSV", "VONV", "FVAL", "SYLD", "IVAL", "GVAL", "QVAL", "PRF", "VLUE", "IVE", "VTV", "USAI", "VBR", "EYLD"],
    "Commos": ["DBC", "DBA", "DBB", "GLD", "SLV", "USO", "GDX", "XLE", "XOP"],
    "Bonds ": ["TLT", "IEF", "LQD", "HYG"],
    "All (basic) securities": basics,
    "Mine": [
        "SPY",
        "QQQ",
        "USAI",
        "2828",
        "EEM",
        "EEMV",
        "PXH",
        "EYLD",
        "EFA",
        "ERUS",
        "RSX",
        "FEZ",
        "EZU",
        "TUR",
        "GREK",
        "XLE",
        "XOP",
        "GDX",
        "NGE",
        "PICK",
        "URA",
        "DGS",
        "IEMG",
        "VNM",
        "FM",
        "EXX1",
        "FYLD",
        "WNRG",
        "MLXIX",
        "EMVL",
    ],
}
reports_colls = [
    "Main Asset Classes",
    "Main Developped",
    "US equities",
    "US sectors",
    "Europe equities",
    "EM equities",
    "Commodity equities",
    "Value",
]


def sub_data1DB(tablename: str):
    res = SQLA_read_table(tablename)
    if "Date" in res.columns:
        res.set_index("Date", inplace=True)
        res.index = res.index.map(lambda s: str(s)[:10])
        res.fillna(method="ffill", inplace=True)
    return res


def report_performance(spots, _type="EQUITY"):
    """
    copied from FinDash_ytdperf
    """
    reportCols = ["Asset", "Last", "1d return", "5d return", "YtD", "1y return", "vs200dma", "vs 52w low", "vs 52w high"]
    cols_headers = reportCols
    spots.index = pd.to_datetime(spots.index)
    spots = spots[spots.index >= "2019-01-01"]
    spots = spots[~spots.index.duplicated(keep="last")]
    df_list = []
    last_bd = spots.index[-1]
    prev_bd = spots.index[-2]
    y_bd = spots.index[-252]
    w_ago = pd.Timestamp(last_bd) - pd.tseries.offsets.BusinessDay(n=5)
    since_pos = spots.index.get_indexer([w_ago], "ffill")[0]  # it is a position
    for ticker in spots.columns:
        prices = spots[ticker]
        today = prices.last_valid_index()
        if today is None:
            pass
        else:
            year = today - pd.tseries.offsets.BYearBegin() - td(1)
            year = prices.index.asof(year)
            close = prices[last_bd]
            prev = prices[prev_bd]

            daily = float((close - prev) / prev * 100.0)
            sincey = float((close - prices[since_pos]) / prices[since_pos] * 100.0)
            ytd = (close - prices[year]) / prices[year] * 100.0
            yret = (close - prices[y_bd]) / prices[y_bd] * 100.0

            dma = prices[-200:].mean()
            dist_dma = (close - dma) / dma * 100.0
            low_52w = prices[-252:].min()
            dist_52low = (close - low_52w) / low_52w * 100.0
            high_52w = prices[-252:].max()
            dist_52high = (close - high_52w) / high_52w * 100.0

            # create temporary frame for current ticker
            df = pd.DataFrame(data=[[ticker, close, daily, sincey, ytd, yret, dist_dma, dist_52low, dist_52high]], columns=cols_headers)
            df_list.append(df)
    res = pd.concat(df_list)
    if _type == "EQUITY":
        decimals = pd.Series([2, 1, 1, 0, 0, 0, 0, 0], index=cols_headers[1:])
    else:
        decimals = pd.Series([4, 1, 1, 0, 0, 0, 0, 0], index=cols_headers[1:])
    res = res.round(decimals)
    return res


def sub_TS(table, unds=""):
    res = sub_data1DB(table)  # includes filling for na
    if unds != "":
        res = res[unds]
    if table == "EQTY_SPOTS":
        perf = report_performance(res, "EQUITY")
        perf.drop([], axis=1, inplace=True)
    else:
        perf = report_performance(res, "FX")
    perf.set_index("Asset", inplace=True)
    for col in ["YtD", "1y return", "vs200dma", "vs 52w low", "vs 52w high"]:
        perf[col] = perf[col].apply(lambda x: str(int(x)) + "%" if (x == x) else "")
    perf["5d return"] = perf["5d return"].apply(lambda x: f"{x/100:.1%}")
    perf["1d return"] = perf["1d return"].apply(lambda x: f"{x/100:.1%}")
    return perf


def IRS_report():
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
    
    data=res[res['Tenor'].isin(tenor_list)]
    pt = pd.pivot_table(data=data, values="Rate", index="Date", columns=["CCY",'Tenor'])
    res = pt.ffill().stack().stack().reset_index().rename(columns={0:'Rate'})
    res=res[['Date','CCY','Tenor','Rate']]
    
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

    return IRSlast, IRSdiff


def govies_report():
    def sub_pivot(bd):
        IRStable = res[res["Date"] == bd]
        IRStable = IRStable[IRStable["nYears"].isin(tenor_list)]
        pivot = pd.pivot_table(data=IRStable, values="Rate", index="nYears", columns="Country")
        pivot = pivot.round(2)
        return pivot

    tenor_list = [2.0, 5.0, 10.0, 30.0]
    res = sub_data1DB("GOVIES_TS")
    res.reset_index(inplace=True)
    dates = res["Date"].unique()
    dates.sort()
    last_bd = dates[-1]
    goviesLast = sub_pivot(last_bd)

    prev_bd = dates[-2]
    week_bd = dates[-6]
    goviesprev = sub_pivot(prev_bd)
    goviesweek = sub_pivot(week_bd)
    goviesdiff1 = (goviesLast - goviesprev).round(2)
    goviesdiff1 = goviesdiff1.applymap(lambda x: f"{x:.2f}").applymap(lambda s: " " * (5 - len(s)) + s)

    goviesdiff2 = (goviesLast - goviesweek).round(2)
    goviesdiff2 = goviesdiff2.applymap(lambda x: f"{x:.2f}").applymap(lambda s: " " * (5 - len(s)) + s)

    goviesDiff = goviesdiff1 + " | " + goviesdiff2

    return goviesLast, goviesDiff


def credit_report():
    df = SQLA_read_table("CREDIT_ETF_TS")
    dfLast = df[df["Date"] == df["Date"].max()]
    return dfLast.set_index("name").loc[:, ["YtM", "OAS"]]


def technicals_report():
    df = SQLA_read_table("TECHNICALS").set_index("Underlying")
    technical_last = df["Date"].max()
    df.drop("Date", inplace=True, axis=1)
    df1 = df.copy()
    df2 = df.copy()
    for col in df.columns.to_list():
        if col[:3] == "pct":
            df1.drop(col, axis=1, inplace=True)
            df2.rename({col: col[4:]}, axis=1, inplace=True)
        else:
            df2.drop(col, axis=1, inplace=True)
    df2.columns.name = "in %"

    htm1 = df1.to_html(classes="table table-striped", col_space="50px", justify="right")
    htm2 = df2.to_html()  # .replace('<table border="1" class="dataframe">','<table class="table table-striped">')
    return htm1, htm2, df1, df2, technical_last


def ETF_ratios_report():
    """loads ratios and spot data from DBs"""
    ETF_ratios = SQLA_read_table("ETF_RATIOS").set_index("index")
    cols = ["Compo_Zscore", "P/E1", "P/B", "P/S", "P/CF", "DY", "Name"]
    dfRatios = ETF_ratios[cols]
    dfRatios = dfRatios.replace(
        ["Select Sector SPDR® Fund", "iShares ", "MSCI ", "SPDR® ", "SPDR ", "ETF", "Vanguard ", "WisdomTree "], "", regex=True
    )
    newcols = ["zscore"] + dfRatios.columns.tolist()[1:]
    dfRatios.columns = newcols
    return dfRatios


def prep_all_data():
    console.log("Getting info for technicals")
    htm1, htm2, df1, df2, technical_last = technicals_report()

    # selects the equity underlyings
    unds = []
    for famille in reports_colls:
        unds += assets_coll[famille]
    unds = list(set(unds))

    # loads data
    console.log("Getting Time Series")
    perf_eq = sub_TS("EQTY_SPOTS")
    perf_ccy = sub_TS("FX_SPOTS")
    perf_commos = sub_TS("COMMOS_SPOTS")
    perf_ccy["Name"] = ""
    perf_commos["Name"] = ""
    dfRatios = ETF_ratios_report()

    # concatenates the EQUITY tables
    console.log("Preparing  Equity data")
    dfEquity = pd.concat([dfRatios, perf_eq], axis=1)
    dfEquity.sort_index(inplace=True)
    cols = perf_eq.columns.to_list()
    cols = cols + ["Name"]
    perf_eq = dfEquity[cols]
    dfEquity.drop(["Last"], axis=1, inplace=True)

    # creates the daily csv used in XL Repartition files for valos
    dfxl = dfEquity.copy()
    dfxl.drop(["1d return", "5d return"], axis=1, inplace=True)
    dfxl.dropna(axis=0, subset=["P/B"], inplace=True)
    try:
        dfxl.to_csv(DIR_FILES + "\\ETF_RATIOS.csv")
        if "ONEDRIVECONSUMER" in os.environ:
            dfxl.to_csv(os.environ["ONEDRIVECONSUMER"] + "\\#Money_top\\ETF_RATIOS.csv")
    except Exception as e:
        print("Error saving file ETF_RATIOS.csv")
        print(e)

    # now only keep relevant unds for the report in the dataframes
    dfEquity = dfEquity[dfEquity.index.isin(unds)]
    dfEquity.index.rename("Asset", inplace=True)
    perf_eq = perf_eq[perf_eq.index.isin(unds)]
    dfRatios = dfRatios.loc[dfRatios.index.intersection(unds)]

    return dfEquity, dfRatios, perf_eq, perf_ccy, perf_commos, technical_last, df1


def create_html_report(dfEquity, dfRatios, perf_eq, perf_ccy, perf_commos, technical_last, df1):
    try:
        res = f"<h2>Technical signals, last imported on {technical_last}</h2><br> "
        res += nice_table(df1, min_chars=10)

    except:
        res = "<h2>Technical signals - ERROR </h2><br> "

    # Cem signals - deprecated
    # lastDate,lastSpot,twentydma,hvol,width,band=technicals.SPXfut()
    # res+=f'<h3>Trading SPX signals</h3>'
    # res+=f'last ES spot {lastSpot}\t 20dMA {twentydma}\t 20d vol {hvol:,.1%} \n\t low band {band[0]}, high band {band[1]}<br><br>'

    """                     IRS   & Govies              """
    try:
        IRSlast, IRSdiff = IRS_report()
        res += "<h2>IRS rates and changes</h2>"
        res += nice_table(IRSlast, min_chars=12, title="Latest swap rates", digits=2)
        res += nice_table(IRSdiff, min_chars=12, title="1d|5d changes", digits=2)
    except:
        res += "<h2>IRS - ERROR </h2><br> "

    try:
        goviesLast, goviesDiff = govies_report()
        res += "<h2>Govies rates and changes</h2>"
        res += nice_table(goviesLast, min_chars=12, title="Latest Govies rates", digits=2)
        res += nice_table(goviesDiff, min_chars=12, title="1d|5d changes", digits=2)
    except:
        res += "<h2>GOVIES - ERROR </h2><br> "

    try:
        creditLast = credit_report()
        res += "<h2>Credit ETF rates and spreads</h2>"
        res += nice_table(creditLast, min_chars=12, title="Latest US Credit rates", digits=2)
    except:
        res += "<h2>CREDIT - ERROR </h2><br> "

    try:
        dfall = pd.concat([perf_eq, perf_ccy, perf_commos], axis=0)
        dfall.index.rename("Asset", inplace=True)
        dfall2 = dfall.copy()
        dfall.dropna(subset=["1d return"], inplace=True)
        dfall["1d return"] = dfall["1d return"].apply(lambda x: float(str(x)[:-1]))
        dfall.sort_values(by="1d return", inplace=True)
        dfall["1d return"] = dfall["1d return"].apply(lambda x: str(x) + "%")
        res += "<h2>Biggest 1 days movers</h2>"
        res += nice_table(dfall.head(5), min_chars=12, title="Losers")
        res += nice_table(dfall.tail(5), min_chars=12, title="Winners")

        dfall = dfall2.copy()
        dfall.dropna(subset=["5d return"], inplace=True)
        dfall["5d return"] = dfall["5d return"].apply(lambda x: float(str(x)[:-1]))
        dfall.sort_values(by="5d return", inplace=True)
        dfall["5d return"] = dfall["5d return"].apply(lambda x: str(x) + "%")
        res += "<h2>Biggest 5 days movers</h2>"
        res += nice_table(dfall.head(5), min_chars=12, title="Losers")
        res += nice_table(dfall.tail(5), min_chars=12, title="Winners")
    except:
        res += "<h2>Equity perf - ERRROR</h2><br>"

    try:
        res += "<br><br>" + "<h2>FX and commos</h2>"
        for dfl in [perf_ccy, perf_commos]:
            dfl.drop(["Name"], axis=1, inplace=True)
            dfl.sort_values("5d return", inplace=True)
        res += nice_table(perf_ccy, min_chars=10, title="FX- USD vs each currency")
        res += "<br>" + nice_table(perf_commos, min_chars=10, title="Commodities")
    except:
        res += "<h2>FX+COMMOS - ERRROR</h2><br>"

    try:
        res += "<br><br>" + "<h2>Valuation ratios and spot moves for major underlyings</h2>"
        for famille in reports_colls:
            try:
                etfs = assets_coll[famille]
                extract = dfEquity.loc[etfs, :]
                extract.sort_values(by="zscore", inplace=True)
                extract.index.rename("Asset", inplace=True)
                tab = nice_table(extract, min_chars=6, title=famille)
                res += tab + "<br>"
            except:
                res += f"<p>ERROR with {famille.upper()} list of underlyings</p>"
    except:
        res += "<h2>Valuation - ERRROR</h2><br>"

    try:
        dfEquity.dropna(subset=["zscore"], inplace=True)
        dfEquity.sort_values(by="zscore", inplace=True)
        res += "<h2>Cheapest and most expensive</h2><br> "
        res += nice_table(dfEquity.head(5), min_chars=6, title="Dearest")
        res += nice_table(dfEquity.tail(5), min_chars=6, title="Cheapest")
    except:
        res += "<h2>Cheapest and most expensive - ERROR</h2><br> "

    return res


def send_report():
    STYLE = "bold blue"
    print("\n\n")
    console.print(
        Rule(
            title="***Preparing email report for markets action ***",
            style="bold blue on white",
        )
    )
    console.log("Getting data...", style=STYLE)
    dfEquity, dfRatios, perf_eq, perf_ccy, perf_commos, technical_last, df1 = prep_all_data()
    console.log("Formatting html report...", style=STYLE)
    body = create_html_report(dfEquity, dfRatios, perf_eq, perf_ccy, perf_commos, technical_last, df1)

    console.log("Sending email...", style=STYLE)
    # send_email("Daily quick mkts report - from server", body)
    send_cyril_andrea("Daily quick mkts report - from Server", body)


if __name__ == "__main__":
    send_report()
