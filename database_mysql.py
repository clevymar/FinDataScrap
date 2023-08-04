import pandas as pd
import pymysql.cursors
from sqlalchemy import create_engine, URL

USERNAME = "CyrilFinanceData"
DB_PWD = "MySQLpwd00" #saved in my.cnf file on PA



def check_tables(conn,cur,table):
    CHECK_QUERY = """
    SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{}'
    """
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
                host=f"{USERNAME}.mysql.eu.pythonanywhere-services.com", 
                database=f'{USERNAME}$Finance',
                # cursorclass=pymysql.cursors.DictCursor
                )
        print('DB connected')
    except Exception as e:
        raise Exception(f"Error connecting to database") from e
    return conn

def get_connection_sqlalchemy():
    engine = None
    try:
        url_object = URL.create(
            "mysql+pymysql",
            username=USERNAME,
            password=DB_PWD, 
            host=f"{USERNAME}.mysql.eu.pythonanywhere-services.com",
            # port = 3306,
            database=f'{USERNAME}$Finance',
            )
        engine = create_engine(url_object)
        print('Connection with SQLAlchemy successful')
        test=pd.read_sql_table('GOVIES_TS',engine)
        print('Existing table\n',test)
    except Exception as e:
        # print(e)
        raise Exception(f"Error connecting to database with SQL Alchemy") from e
    return engine



def SQL_update(df,tablename,mode="replace",idx=True,verbose=True):
    engine=get_connection_sqlalchemy()
    try:
        df.to_sql(tablename, con=engine, if_exists=mode,index=idx)
        if verbose: print(f" {len(df)} records saved to {tablename}")
    except Exception as e:
        print(e)
        raise Exception(f"Error saving {tablename} to SQL DB") from e

# def DB_last_date(tablename,include_data=False):
#     engine=get_connection()
#     sql = f""" select max(Date) from {tablename} """
#     with engine:
#         cur = engine.cursor()
#         cur.execute(sql)
#         rows = cur.fetchall()
#     last_date = rows[0][0]
    
#     cnx = get_connection_sqlalchemy()
#     df=pd.read_sql_query(f"SELECT * FROM {tablename}" , cnx)
#     df=df[df['Date']==last_date]
#     print(df)
#     return last_date



if __name__ == "__main__":

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            check_tables(conn,cur,'GOVIES_TS')
    except Exception as e:
        raise Exception(f"Error writing to database") from e
    finally:
        conn.close
    
