import datetime
import pandas as pd

tod = datetime.date.today().strftime("%Y-%m-%d")
start = datetime.datetime(2007, 1, 1)
end = datetime.date.today()-pd.tseries.offsets.BDay(1)+pd.tseries.offsets.Hour(23)+pd.tseries.offsets.Minute(59)
last_bd = end.strftime("%Y-%m-%d")


EQUITY_UNDS = ['AAXJ', 'ACWI', 'AFK', 'ARGT', 'ARKK', 'BND', 'BNO', 'BWX', 'BWZ', 'CGGO', 'CGW', 'COPX', 'CORN', 'CYB', 'DBA', 'DBB', 'DBC', 'DEM', 'DFAX', 'DFEV', 'DFIV', 'DFLV', 'DFSV', 'DGS', 'DISV', 'DJP', 'ECOW', 'EEM', 'EEMV', 'EFA', 'EFG', 'EFV', 'EIDO', 'EIRL', 'EIS', 'EMLC', 'ENZL', 'EPHE', 'EPOL', 'EPP', 'EPU', 'ERUS', 'EUFN', 'EWA', 'EWC', 'EWD', 'EWG', 'EWH', 'EWI', 'EWJ', 'EWK', 'EWL', 'EWM', 'EWN', 'EWO', 'EWP', 'EWQ', 'EWS', 'EWT', 'EWU', 'EWW', 'EWY', 'EWZ', 'EYLD', 'EZA', 'EZU', 'FEZ', 'FM', 'FVAL', 'FXA', 'FXB', 'FXC', 'FXE', 'FXF', 'FXI', 'FXY', 'FYLD', 'GDX', 'GDXJ', 'GLD', 'GNR', 'GREK', 'GVAL', 'GXG', 'HAP', 'HYG', 'IDEV', 'IEF', 'IEMG', 'IEUR', 'IEV', 'IHY', 'ILF', 'INDY', 'IUSV', 'IVAL', 'IVE',
               'IWB', 'IWC', 'IWD', 'IWF', 'IWL', 'IWM', 'IWN', 'IWO', 'IWP', 'IWR', 'IWS', 'IYR', 'JKL', 'JNK', 'KBE', 'KCE', 'KIE', 'KRE', 'KWEB', 'LQD', 'MGK', 'MLXIX', 'NGE', 'NIB', 'NORW', 'OIH', 'PALL', 'PICB', 'PICK', 'PPLT', 'PRF', 'PXF', 'PXH', 'QQQ', 'QVAL', 'REM', 'RSX', 'SCJ', 'SHV', 'SHY', 'SIL', 'SLV', 'SOXX', 'SOYB', 'SPY', 'SYLD', 'THD', 'TIP', 'TLT', 'TUR', 'UDN', 'UGA', 'UNG', 'URA', 'USAI', 'USO', 'UUP', 'VBR', 'VCIT', 'VCLT', 'VCSH', 'VEA', 'VGK', 'VLUE', 'VNM', 'VNQ', 'VNQI', 'VONV', 'VOX', 'VPL', 'VTI', 'VTV', 'VWO', 'WEAT', 'WIP', 'XAR', 'XBI', 'XES', 'XHB', 'XHE', 'XHS', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XME', 'XOP', 'XPH', 'XRT', 'XSD', 'XTN', '^HSCE', '^N225', '^STOXX50E']


# EQUITY_UNDS = ['AAXJ', 'ACWI', 'AFK', 'ARGT', 'ARKK', 'BND', 'BNO', 'IWB', 'IWC', 'IWD',
#                'IEF', 'IEMG', 'IEUR', 'IEV', 'IHY', 'ILF', 'INDY', 'IUSV', 'IVAL', 'IVE',]
