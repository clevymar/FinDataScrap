import pandas as pd
import sys
sys.path.insert(0, '..')

from sqlalchemy import text

from database_connect import PADB_connection
from database_mysql import SQLA_read_table

def list_tables():
    with PADB_connection() as engine:
        with engine.connect() as connection:
            result = connection.execute(text("SHOW TABLES"))
            tables = result.fetchall()
            for table in tables:
                tablename = table[0]
                df = SQLA_read_table(tablename)
                if 'Date' in df.columns or 'date' in df.columns:
                    msgD='TS'
                else:
                    msgD='Snapshot'
                print(f"{tablename}\t- {len(df)} rows\t{msgD}")
                
   
list_tables() 
