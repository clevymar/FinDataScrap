import pandas as pd
import sys
sys.path.insert(0, '..')

from sqlalchemy import text

from database_connect import PADB_connection
from database_sqlite import DB_update
from utils.utils import print_color


"""  UPDATE PROCEDURES """

def SQLA_update(df,tablename,mode="replace",idx=True,verbose=True):
    with PADB_connection() as engine:
        try:
            df.to_sql(tablename, con=engine, if_exists=mode,index=idx)
            if verbose: print(f" {len(df)} records saved with pandas to SQL DB table {tablename}")
        except Exception as e:
            errorMsg = f"Error saving {tablename} to SQL DB"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e

def databases_update(df:pd.DataFrame,tablename:str,mode:str="replace",idx:bool=True,verbose:bool=True, save_insqlite:bool=True):
    SQLA_update(df,tablename,mode=mode,idx=idx,verbose=verbose)
    if save_insqlite:
        DB_update(df,tablename,mode=mode,idx=idx,verbose=verbose)




""" READ PROCEDURES """""

def SQLA_last_date(tablename:str, field:str='Date'):
    with PADB_connection() as engine:
        try:
            sql = f""" select max({field}) from {tablename} """
            tmp = pd.read_sql_query(sql , engine)
            return tmp.iloc[0,0]
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e
    
    
def SQLA_dates(tablename:str):
    sql = text(f""" SELECT DISTINCT Date from {tablename} ORDER BY Date desc""")
    with PADB_connection() as engine:
        with engine.connect() as connection:
            result = connection.execute(sql)
            rows = result.fetchall()
    return [rows[0][0],rows[1][0]]


    
def SQLA_read_table(tablename:str,retrieve_only_info_for_last_date:bool=False):
    with PADB_connection() as engine:
        try:
            if retrieve_only_info_for_last_date:
                sql = f""" select * from {tablename} where Date = (select max(Date) from {tablename}) """
            else:
                sql = f""" select * from {tablename} """
            tmp = pd.read_sql_query(sql , engine)
            return tmp
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e
    






