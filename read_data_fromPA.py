# from mysql.connector import connect, Error  # not sure why but does not work, pymysql does
import pandas as pd
import sshtunnel
import pymysql.cursors
from sqlalchemy import create_engine, URL
from database_mysql import get_connection,get_connection_sqlalchemy

from credentials import USERNAME, DB_PWD, PA_PWD
from utils import isLocal


def DB_last_date(engine,tablename,include_data=False):
    sql = f""" select max(Date) from {tablename} """
    cur = engine.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    last_date = rows[0][0]
    
#     cnx = get_connection_sqlalchemy()
#     df=pd.read_sql_query(f"SELECT * FROM {tablename}" , cnx)
#     df=df[df['Date']==last_date]
#     print(df)
    return last_date

def sshserver():
    sshtunnel.SSH_TIMEOUT = 5.0
    sshtunnel.TUNNEL_TIMEOUT = 5.0

    server =   sshtunnel.SSHTunnelForwarder(
    ('ssh.eu.pythonanywhere.com'),
    ssh_username=USERNAME, #PA login
    ssh_password=PA_PWD,
    remote_bind_address=(f'{USERNAME}.mysql.eu.pythonanywhere-services.com', 3306),
                                )
    server.start() #TODO dont forget to close it !
    print(f'Tunnel created at port {server.local_bind_port}')
    return server
    
def PADB_connect(tunnel):    
    conn = pymysql.connect( 
            user=USERNAME, #PA database username
            password=DB_PWD,
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=f'{USERNAME}$Finance',
            # cursorclass=pymysql.cursors.DictCursor
            )
    # Do stuff
    print('DB connected')
    return conn

def PADB_connection_sqlalchemy(tunnel):
    engine = None
    try:
        url_object = URL.create(
            "mysql+pymysql",
            username=USERNAME,
            password=DB_PWD, 
            host='127.0.0.1',
            port = tunnel.local_bind_port,
            database=f'{USERNAME}$Finance',
            )
        engine = create_engine(url_object)
        print('Connection to SQLAlchemy successful')
    except Exception as e:
        # print(e)
        raise Exception(f"Error connecting to database with SQL Alchemy") from e
    return engine


def explore(conn,sqlalchemycon):
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        for row in cur.fetchall():
            print(row)
    
    for table in ['GOVIES_TS','IRS_TS']:
        lastDate = DB_last_date(conn,table)
        print(f"Last date in {table} is {lastDate}")
            
        df=pd.read_sql_query(f"SELECT * FROM {table}" , sqlalchemycon)
        df=df[df['Date']==lastDate]
        print(df)
            

def PADB_run_task(func,run_local=True):
    
    if run_local:
        server = sshserver()
        conn = PADB_connect(server)
        cnx = PADB_connection_sqlalchemy(server)
    else:
        conn = get_connection()
        cnx = get_connection_sqlalchemy()

    try:    
        func(conn,cnx)
    except Exception as e:
        raise Exception(f'Error whilst running {func}') from e
    finally:
        conn.close()
        if run_local: server.close()

    
if __name__ == "__main__":
    PADB_run_task(explore,isLocal())
    
