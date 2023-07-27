
import pandas as pd
import sshtunnel
import pymysql.cursors
from sqlalchemy import create_engine, URL


from database import DB_FOLDER
from credentials import USERNAME, DB_PWD, PA_PWD
HOST = '127.0.0.1'

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

SQL_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
	Date VARCHAR(10),
    nYears FLOAT,
	Country VARCHAR(50),
	Rate FLOAT
);

"""

CHECK_QUERY = """
SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}'
"""

def create_table(conn,cur,table:str):
    cur.execute(SQL_QUERY.format(table))
    conn.commit()
    print('Query commited')
        
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

def initial_table_setup(tablename:str, erase:bool=False):
    conn = pymysql.connect( 
            user=USERNAME, #PA database username
            password=DB_PWD,
            host=HOST, port=tunnel.local_bind_port,
            database=f'{USERNAME}$Finance',
            cursorclass=pymysql.cursors.DictCursor)
    # Do stuff
    print('DB connected')
    with conn.cursor() as cur:
        if erase:
            cur.execute(f"DROP TABLE IF EXISTS `{tablename}`;")
        create_table(conn,cur,tablename)
        check_tables(conn,cur,tablename)
    print('Closing connection')
    conn.close()





with sshtunnel.SSHTunnelForwarder(
    ('ssh.eu.pythonanywhere.com'),
    ssh_username=USERNAME, #PA login
    ssh_password=PA_PWD,
    remote_bind_address=(f'{USERNAME}.mysql.eu.pythonanywhere-services.com', 3306),
                                ) as tunnel:
    print(f'Tunnel setup at port {tunnel.local_bind_port}')
    
    initial_table_setup('GOVIES_TS',erase=True)
    
    url_object = URL.create(
        "mysql+pymysql",
        username=USERNAME,
        password=DB_PWD,  # plain (unescaped) text
        host=HOST,
        port = tunnel.local_bind_port,
        database=f'{USERNAME}$Finance',
        )
    engine = create_engine(url_object)
    test=pd.read_sql_table('GOVIES_TS',engine)
    print('Existing table\n',test)

    df=pd.read_csv(DB_FOLDER + 'GOVIES_TS.csv')
    df.to_sql('GOVIES_TS',engine,if_exists='append',index=False)
    test=pd.read_sql_table('GOVIES_TS',engine)
    print('New table\n',test)

    

    
    
