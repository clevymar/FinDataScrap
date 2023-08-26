import pandas as pd
import pymysql.cursors

from database_connect import PADB_connection
from database_sqlite import DB_update
from utils import print_color


# def check_tables(conn,cur,table):
#     CHECK_QUERY = """
#     SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE 
#     FROM INFORMATION_SCHEMA.COLUMNS
#     WHERE TABLE_NAME = '{}'
#     """
#     cur.execute("SHOW TABLES")
#     for row in cur.fetchall():
#         print(row)
#     cur.execute(CHECK_QUERY.format(table))
#     lstColumns=[]
#     for row in cur.fetchall():
#         lstColumns.append(row)
#     lstColumns = sorted(lstColumns,key=lambda d: d['ORDINAL_POSITION'])
#     for col in lstColumns:
#         print(f"{col['ORDINAL_POSITION']} - {col['COLUMN_NAME']} - {col['DATA_TYPE']}")



def SQLA_update(df,tablename,mode="replace",idx=True,verbose=True):
    with PADB_connection() as engine:
        try:
            df.to_sql(tablename, con=engine, if_exists=mode,index=idx)
            if verbose: print(f" {len(df)} records saved with Alchemy to {tablename}")
        except Exception as e:
            errorMsg = f"Error saving {tablename} to SQL DB"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e

def databases_update(df:pd.DataFrame,tablename:str,mode:str="replace",idx:bool=True,verbose:bool=True, save_insqlite:bool=True):
    SQLA_update(df,tablename,mode=mode,idx=idx,verbose=verbose)
    if save_insqlite:
        DB_update(df,tablename,mode=mode,idx=idx,verbose=verbose)


def SQLA_last_date(tablename):
    with PADB_connection() as engine:
        try:
            sql = f""" select max(Date) from {tablename} """
            tmp = pd.read_sql_query(sql , engine)
            return tmp.iloc[0,0]
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e
    
    
def SQLA_read_table(tablename):
    with PADB_connection() as engine:
        try:
            sql = f""" select * from {tablename} """
            tmp = pd.read_sql_query(sql , engine)
            return tmp
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e
    




