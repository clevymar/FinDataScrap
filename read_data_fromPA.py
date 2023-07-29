# from mysql.connector import connect, Error  # not sure why but does not work, pymysql does
import pandas as pd
import sshtunnel
import pymysql.cursors

from credentials import USERNAME, DB_PWD, PA_PWD

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

# server = sshtunnel.open_tunnel(
#     ('ssh.eu.pythonanywhere.com'),
#     ssh_username=USERNAME, #PA login
#     ssh_password='Python00clm#',
#     remote_bind_address=(f'{USERNAME}.mysql.eu.pythonanywhere-services.com', 3306),
#     debug_level='TRACE',
# )

# server.start()
# print(f"\n\n{server.local_bind_port=}")  # show assigned local port
# server.stop()

# exit(0)

with sshtunnel.SSHTunnelForwarder(
    ('ssh.eu.pythonanywhere.com'),
    ssh_username=USERNAME, #PA login
    ssh_password=PA_PWD,
    remote_bind_address=(f'{USERNAME}.mysql.eu.pythonanywhere-services.com', 3306),
                                ) as tunnel:
    print(f'Tunnel setup at port {tunnel.local_bind_port}')
    
    # conn = connect(
    #     user=USERNAME, #PA database username
    #     password=DB_PWD,
    #     host='127.0.0.1', port=tunnel.local_bind_port,
    #     database=f'{USERNAME}$Finance',
    # )
    
    conn = pymysql.connect( 
            user=USERNAME, #PA database username
            password=DB_PWD,
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=f'{USERNAME}$Finance',
            cursorclass=pymysql.cursors.DictCursor)
    # Do stuff
    print('DB connected')
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        for row in cur.fetchall():
            print(row)
    # print(conn.is_connected())
    
    print('Closing connection')
    conn.close()