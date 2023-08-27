# from mysql.connector import connect, Error  # not sure why but does not work, pymysql does
import pandas as pd

from utils import isLocal, print_color
from database_connect import PADB_connection
from database_mysql import SQLA_read_table, SQLA_last_date

def DB_last_date(engine,tablename,include_data=False):
    sql = f""" select max(Date) from {tablename} """
    cur = engine.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    last_date = rows[0][0]
    return last_date

def SQLA_last_date(sqlalchemycon,tablename):
    sql = f""" select max(Date) from {tablename} """
    tmp = pd.read_sql_query(sql , sqlalchemycon)
    # last_date = rows[0][0]
    return tmp.iloc[0,0]



def explore(sqlalchemycon):
    
    for table in ['GOVIES_TS','TECHNICALS']:
        lastDate = SQLA_last_date(sqlalchemycon,table)
        print(f"\n\nLast date in {table} is {lastDate}")
            
        df=pd.read_sql_query(f"SELECT * FROM {table}" , sqlalchemycon)
        df=df[df['Date']==lastDate]
        print(df)
            



# def PADB_run_task(func,run_local=True):
#     if run_local:
#         server = sshserver()
#         conn = PADB_connect(server)
#         cnx = PADB_connection_sqlalchemy(server)
#     else:
#         conn = get_connection()
#         cnx = get_connection_sqlalchemy()

#     try:    
#         func(conn,cnx)
#     except Exception as e:
#         raise Exception(f'Error whilst running {func}') from e
#     finally:
#         conn.close()
#         if run_local: server.close()

    
if __name__ == "__main__":
    # PADB_run_task(explore,isLocal())
    with PADB_connection() as sqlalchemycon:
        # temp = SQLA_last_date(sqlalchemycon,'GOVIES_TS')
        # print(temp)
        explore(sqlalchemycon)
        # print(SQLA_read_table('GOVIES_TS'))
    latest = SQLA_read_table('ETF_RATIOS', retrieve_only_info_for_last_date=True)
    print(f"ETF RATIOS shape: {latest.shape}")
    
    
    
