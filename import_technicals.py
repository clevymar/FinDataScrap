import urllib.request
import pandas as pd

from utils import timer, isLocal
from database_mysql import SQLA_last_date, databases_update
from classes import Scrap
from common import last_bd

unds = [
    ["Gold", "GLD"],
    ["SPX", "$SPX"],
    ["NDX", "NQ*0"],
    ["Dollar", "DX*0"],
    ["Estoxx", "FX*0"],
    ["Commos", "DBC"],
    ["Bitcoin", "BT*0"],
]

user_agent = "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7"
headers = {"User-Agent": user_agent}

def one_technical(und_row):
    und = und_row[1]
    name = und_row[0]
    url = f"https://www.barchart.com/etfs-funds/quotes/{und}/technical-analysis"
    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    data = response.read()  # The data u need

    dfs = pd.read_html(data)

    dma20 = dfs[0].iloc[1, 1]
    dma50 = dfs[0].iloc[2, 1]
    dma200 = dfs[0].iloc[4, 1]
    RSI14 = round(float(str(dfs[2].iloc[1, 1])[:-1]), 0)

    tab = [name]
    tab = tab + [dma20, dma50, dma200, RSI14]
    return tab


def one_last_and_Fibo(und):
    url = f"https://www.barchart.com/futures/quotes/{und}/overview"
    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    data = response.read()  # The data u need
    dfs = pd.read_html(data)
    df = dfs[len(dfs) - 1]
    df.set_index(0, inplace=True)
    return df


def one_all(und_row):
    und = und_row[1]
    print(f"Processing {und} - {und_row[0]} ")
    # name=und_row[0]
    df1 = one_last_and_Fibo(und)
    last = df1.loc["Last Price", 1]
    if str(last)[-1] == "s":
        last = last.replace(",", "")
        last = float(last[:-1])
        df1.loc["Last Price", 1] = last
    tab = one_technical(und_row)
    df2 = pd.DataFrame([["Underlying", "20d MA", "50d MA", "200d MA", "14d RSI"], tab])
    df2.columns = df2.iloc[0]
    df2.drop(df2.index[0], inplace=True)
    df1 = df1.T
    df = pd.concat([df1, df2], axis=1)
    df.set_index("Underlying", inplace=True)
    df.columns.name = "Technicals"
    df = df[
        [
            "Last Price",
            "52-Week High",
            "Fibonacci 61.8%",
            "Fibonacci 50%",
            "Fibonacci 38.2%",
            "52-Week Low",
            "20d MA",
            "50d MA",
            "200d MA",
            "14d RSI",
        ]
    ]

    for col in [
        "52-Week High",
        "Fibonacci 61.8%",
        "Fibonacci 50%",
        "Fibonacci 38.2%",
        "52-Week Low",
        "20d MA",
        "50d MA",
        "200d MA",
    ]:
        df["pct_" + col] = df[col].astype("float") / last * 100
        df["pct_" + col] = df["pct_" + col].astype("float").round(1)
    return df


def create_technicals():
    tab=[]
    for und_row in unds:
        dft = one_all(und_row)
        tab.append(dft)
    df=pd.concat(tab)
    print(df.T)
    # df.to_csv(dir_main + "technicals.csv")
    return df

@timer
def technicals_toDB(verbose=False):
    df=create_technicals()
    df['Date']=last_bd
    if verbose:
        print(df)
    databases_update(df.reset_index(),"TECHNICALS",idx=False,mode='replace',verbose=verbose, save_insqlite=True)
    return df
    
def import_technicals(verbose=True):
    msg=None
    try:
        res = technicals_toDB(verbose=verbose)
        msg = f'Well downloaded !!! \n{len(res)} rows, {len(res.columns)} cols'
    except Exception as e:
        raise Exception('Error while downloading and computing Technicals') from e
    return msg

def technicals_last_date():
    return SQLA_last_date("TECHNICALS")

ScrapTechnicals = Scrap("Technicals", technicals_toDB, technicals_last_date)


# def SPXfut():
#     end = datetime.date.today()
#     start = end - datetime.timedelta(days=365)

#     res = time_serie_Yahoo("ES=F", start, end, "Close")

#     lastDate = str(res.index[-1])[:10]
#     tod = str(date.today())[:10]
#     lastSpot = res[-1]
#     if lastDate == tod:
#         res = res.iloc[:-1]
#     sma = res.rolling(20).mean()
#     twentydma = round(sma[-1], 2)
#     logret = np.log(res / res.shift(1))
#     hvol = logret.rolling(window=20).std() * np.sqrt(252)
#     width = hvol[-1] / np.sqrt(252)
#     band = []
#     band.append(round(twentydma * (1 - width), 2))
#     band.append(round(twentydma * (1 + width), 2))
#     return lastDate, lastSpot, twentydma, hvol[-1], width, band


""" ****************************************
                formatting 
"""


# def clm_format_clean(val):
#     if isinstance(val, (float, int)):
#         res = f"{val:,.2f}".rstrip("0").rstrip(".")  # f"{val:g}"
#     else:
#         res = val
#     return res


# def clm_html_table(df, red=False, show=False, first_col=False):
#     vals = df.reset_index()
#     vals = vals.applymap(clm_format_clean)
#     if red:
#         font_color = [
#             [
#                 "red" if type(v) != str and v <= 0 else "black"
#                 for v in vals[col].tolist()
#             ]
#             for col in vals.columns[1:]
#         ]
#         font_color[0] = ["blue" for v in range(len(font_color[0]))]
#     else:
#         font_color = "black"
#     colw = 860
#     if first_col:
#         start = 0
#     else:
#         start = 1
#     nbc = len(vals.columns[start:]) - 1
#     colwidth = [2 * colw] + [colw] * nbc
#     # print(vals.columns[start:])
#     fig = go.Figure(
#         data=[
#             go.Table(
#                 header=dict(
#                     values=list(vals.columns[start:]),
#                     fill_color="royalblue",
#                     font=dict(color="white", size=12),
#                     height=40,
#                     align=["left"] + ["right"] * nbc,
#                 ),
#                 cells=dict(
#                     values=vals.T.values.tolist()[start:],
#                     fill_color="lightgrey",
#                     font=dict(color=font_color),
#                     align=["left"] + ["right"] * nbc,
#                 ),
#                 columnwidth=colwidth,
#             )
#         ]
#     )
#     if show:
#         fig.show(renderer="browser")
#     return fig


# def panda_nice_html(df, title=""):
#     """
#     Write an entire dataframe to an HTML file with nice formatting.
#     """

#     result = """
#                 <html>
#                 <head>
#                 <style>

#                     h2 {
#                         text-align: center;
#                         font-family: Helvetica, Arial, sans-serif;
#                     }
#                     table { 
#                         margin-left: auto;
#                         margin-right: auto;
#                     }
#                     table, th, td {
#                         border: 1px solid green;
#                         border-collapse: collapse;
#                     }
#                     th, td {
#                         padding: 5px;
#                         text-align: center;
#                         font-family: Helvetica, Arial, sans-serif;
#                         font-size: 90%;
#                     }
#                     table tbody tr:hover {
#                         background-color: #dddddd;
#                     }
#                     .wide {
#                         width: 90%; 
#                     }

#                 </style>
#                 </head>
#                 <body>
#                     """
#     result += "<h2> %s </h2>\n" % title
#     # if type(df) == pd.io.formats.style.Styler:
#     #    result += df.render()
#     # else:
#     result += df.to_html(classes="wide", escape=False)
#     result += """
#                 </body>
#                 </html>
#                 """
#     return result


# def show_result_html(df):
#     df1 = df.copy()
#     df2 = df.copy()
#     for col in df.columns.to_list():
#         if col[:3] == "pct":
#             df1.drop(col, axis=1, inplace=True)
#             df2.rename({col: col[4:]}, axis=1, inplace=True)
#         else:
#             df2.drop(col, axis=1, inplace=True)
#     df2.columns.name = "in %"

#     fig1 = clm_html_table(df1, red=True, show=False, first_col=True)
#     fig2 = clm_html_table(df2, red=True, show=False, first_col=True)

#     return fig1, fig2


# def show_result_email():
#     df = pd.read_csv(dir_main + "technicals.csv", index_col=0)
#     df1 = df.copy()
#     df2 = df.copy()
#     for col in df.columns.to_list():
#         if col[:3] == "pct":
#             df1.drop(col, axis=1, inplace=True)
#             df2.rename({col: col[4:]}, axis=1, inplace=True)
#         else:
#             df2.drop(col, axis=1, inplace=True)
#     df2.columns.name = "in %"

#     htm1 = df1.to_html(classes="table table-striped", col_space="50px", justify="right")
#     htm2 = (
#         df2.to_html()
#     )  # .replace('<table border="1" class="dataframe">','<table class="table table-striped">')
#     return htm1, htm2, df1, df2


# """ ****************************************
#                 website 
                
# """



if __name__ == "__main__":
    print(import_technicals(verbose=True))
