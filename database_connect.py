import pymysql.cursors
from sqlalchemy import create_engine, URL
from contextlib import contextmanager

from credentials import USERNAME, DB_PWD, PA_PWD
from utils import isLocal, print_color
if isLocal(): import sshtunnel

def print_dbmessage(msg):
    print_color("[DB] - "+msg,'DBCONNECT')

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
    print_dbmessage(f'Tunnel created at port {server.local_bind_port}')
    return server
    
def PADB_connection_MYSQL(tunnel):    
    conn = pymysql.connect( 
            user=USERNAME, #PA database username
            password=DB_PWD,
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=f'{USERNAME}$Finance',
            # cursorclass=pymysql.cursors.DictCursor
            )
    print_dbmessage('DB connected')
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
        print_dbmessage('Connection with SQLAlchemy successful')
    except Exception as e:
        print_color(e,'FAIL')
        raise Exception(f"Error connecting to database with SQL Alchemy") from e
    return engine


def direct_connection_sqlalchemy():
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
    except Exception as e:
        print_color(e,'FAIL')
        raise Exception(f"Error connecting to database with SQL Alchemy") from e
    return engine


@contextmanager
def PADB_connection():
    run_local = isLocal()
    if run_local:
        server = sshserver()
        # conn = PADB_connect(server)
        cnx = PADB_connection_sqlalchemy(server)
    else:
        # conn = get_connection()
        cnx = direct_connection_sqlalchemy()
    try:
        yield cnx
    except Exception as e:
        raise Exception(f'Error whilst creating connections') from e
    finally:
        if run_local: server.close()
