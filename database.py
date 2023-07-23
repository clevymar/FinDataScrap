import os
import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np

from utils import isLocal


if isLocal():
    """ for PC development """
    PYTHONPATH=os.environ.get('ONEDRIVECONSUMER') + "\\Python Scripts\\"
    DB_FOLDER=PYTHONPATH + "Financial Files\\"
else:

    """ for online python anywhere version """
    DB_FOLDER=u"/home/CyrilFinanceData/Files/"

DB_PATH = DB_FOLDER + "Finance.db"


def create_connection():
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(DB_PATH)
        print("Connected - SQLite version : " + sqlite3.version)
        return conn
    except Error as e:
        raise ConnectionAbortedError('Could not connect to the DB') from e



def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def read_table(table):
    conn=create_connection()
    df=pd.read_sql_query(f"select * from {table}",conn)
    #print(df)
    return df


def DB_date_exists(tablename,d):
    engine=create_connection()
    sql = f"""SELECT count(*) FROM {tablename} WHERE Date = ? """
    with engine:
        cur = engine.cursor()
        cur.execute(sql,(d,))
    data=cur.fetchone()[0]
    if data == 0:
        return False
    else :
        return True

def DB_last_date(tablename,field="Date"):
    engine=create_connection()
    sql = f""" select max({field}) from {tablename} """
    with engine:
        cur = engine.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

    return rows[0][0]

def DB_doublon(tablename):
    engine=create_connection()
    sql = f""" SELECT Date, COUNT(*) c FROM {tablename} GROUP BY Date HAVING c > 1 """
    with engine:
        cur = engine.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    res=len(rows)
    if res==0:
        print(f"No duplicate for {tablename}")
    else:
        print(f"{res} duplicate for {tablename}")
        for row in rows:
            print(row)
    return res

def DB_differential(tablename, fields):
    cnx=create_connection()
    df=pd.read_sql_query("SELECT * FROM " + tablename, cnx)
    df.sort_index(ascending=False,inplace=True)
    df.replace([None], np.nan, inplace=True)
    for f in fields:
        df[f+"_diff"]=(df[f]-df[f].shift(-1)).fillna(0)
        df[f+"_diff"]=df[f+"_diff"].apply(lambda x:int(x))

    return df

def list_tables():
    engine=create_connection()
    print("Hello DB")
    with engine:
        cur = engine.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        rows = cur.fetchall()
    res=len(rows)
    tab=[]
    if res==0:
        print(f"No tables")
    else:
        for row in rows:
            #print(row)
            tab.append(row[0])
        print(tab)
    return tab

def DB_print_heads():
    tables=list_tables()
    print("\n"*5)
    cnx=create_connection()
    for tablename in tables:
        print(f'\n\t*** {tablename}')
        try:
            df=pd.read_sql_query("SELECT * FROM " + tablename, cnx)
            df.sort_values("Date",ascending=False,inplace=True)
            print(df.head())
        except:
            if tablename=="FITBIT_HR_INTRA":
                df=pd.read_sql_query("SELECT * FROM " + tablename, cnx)
                df.sort_values("Datetime",ascending=False,inplace=True)
                print(df.head())
            else:
                print("incorrect table")




def delete_task(conn,table,field, id):
    """
    Delete a task by task id
    :param conn:  Connection to the SQLite database
    :param id: id of the task
    """
    sql = f'DELETE FROM {table} WHERE {field}=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()


if __name__ == "__main__":
    #print(DB_date_exists("RESCUETIME","2019-02-01"))
    #DB_doublon("TODOIST_RAW")
    #print(DB_differential("EVERNOTE",["Total notes","Book summaries"]))
    list_tables()
    #print("\n"*3+DB_last_date("FITBIT_BODY"))

