import time
import sys

sys.path.insert(0, "..")

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import TimeoutError, OperationalError

from database_connect import PADB_connection
from database_sqlite import DB_update
from utils.utils import print_color


"""  UPDATE PROCEDURES """


def SQLA_update(df, tablename, mode="replace", idx=True, verbose=True):
    max_retries = 3
    retry_delay = 60
    try:
        for attempt in range(max_retries):
            try:
                with PADB_connection() as engine:
                    df.to_sql(tablename, con=engine, if_exists=mode, index=idx)
                    if verbose:
                        print(f" {len(df)} records saved with pandas to SQL DB table {tablename}")
                    break
            except (TimeoutError, OperationalError) as e:
                print(f"TimeoutError occurred: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    errorMsg = f"Error saving {tablename} to SQL DB even after {max_retries} attempts"
                    print_color(errorMsg, "FAIL")
                    raise Exception(errorMsg) from e
    except Exception as e:
        errorMsg = f"Error saving {tablename} to SQL DB"
        print_color(errorMsg, "FAIL")
        raise Exception(errorMsg) from e


def databases_update(df: pd.DataFrame, tablename: str, mode: str = "replace", idx: bool = True, verbose: bool = True, save_insqlite: bool = True):
    SQLA_update(df, tablename, mode=mode, idx=idx, verbose=verbose)
    if save_insqlite:
        DB_update(df, tablename, mode=mode, idx=idx, verbose=verbose)


""" READ PROCEDURES """ ""


def SQLA_last_date(tablename: str, field: str = "Date") -> str:
    with PADB_connection() as engine:
        try:
            sql = f""" select max({field}) from {tablename} """
            tmp = pd.read_sql_query(sql, engine)
            return str(tmp.iloc[0, 0])
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e


def SQLA_dates(tablename: str)->list[str]:
    sql = text(f""" SELECT DISTINCT Date from {tablename} ORDER BY Date desc""")
    with PADB_connection() as engine:
        with engine.connect() as connection:
            result = connection.execute(sql)
            rows = result.fetchall()
    return [rows[0][0], rows[1][0]]


def SQLA_read_table(tablename: str, retrieve_only_info_for_last_date: bool = False) -> pd.DataFrame:
    with PADB_connection() as engine:
        try:
            if retrieve_only_info_for_last_date:
                sql = f""" select * from {tablename} where Date = (select max(Date) from {tablename}) """
            else:
                sql = f""" select * from {tablename} """
            tmp = pd.read_sql_query(sql, engine)
            return tmp
        except Exception as e:
            errorMsg = f"Error getting info from {tablename}"
            print_color(errorMsg, "FAIL")
            raise Exception(errorMsg) from e


if __name__ == "__main__":
    print(SQLA_last_date("IRS_TS"))
