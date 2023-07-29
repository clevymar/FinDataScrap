import pandas as pd
import pymysql.cursors
from sqlalchemy import create_engine, URL

USERNAME = "CyrilFinanceData"
DB_PWD = "MySQLpwd00" #saved in my.cnf file on PA


def check_tables(conn,cur,table):
    cur.execute("SHOW TABLES")
    for row in cur.fetchall():
        print(row)
    cur.execute(CHECK_QUERY.format(table))
    lstColumns=[]
    for row in cur.fetchall():
        lstColumns.append(row)
    lstColumns = sorted(lstColumns,key=lambda d: d['ORDINAL_POSITION'])
    for col in lstColumns:
        print(f"{col['ORDINAL_POSITION']} - {col['COLUMN_NAME']} - {col['DATA_TYPE']}")

def get_connection():

    conn = None
    try:
        conn = pymysql.connect( 
                user=USERNAME, #PA database username
                password=DB_PWD,
                host="{USERNAME}.mysql.eu.pythonanywhere-services.com", 
                database=f'{USERNAME}$Finance',
                cursorclass=pymysql.cursors.DictCursor)
        print('DB connected')
    except Exception as e:
        raise Exception(f"Error connecting to database") from e
    
    return conn
    # Do stuff
    
    # with conn.cursor() as cur:
    #     if erase:
    #         cur.execute(f"DROP TABLE IF EXISTS `{tablename}`;")
    #     create_table(conn,cur,tablename)
    #     check_tables(conn,cur,tablename)
    # print('Closing connection')
    # conn.close()

conn = get_connection()
try:
    with conn.cursor() as cur:
        check_tables(conn,cur,'GOVIES_TS')
except Exception as e:
    raise Exception(f"Error writing to database") from e
finally:
    conn.close
    
