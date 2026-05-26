import pandas as pd
from loguru import logger

from database_connect import PADB_connection


with PADB_connection() as conn:
    # Select the record where index = "GXG"
    df = pd.read_sql_query("SELECT * FROM ETF_RATIOS WHERE `index` = 'GXG'", conn)
    if not df.empty:
        # Replace index value and update URL
        df["index"] = "COLO"
        df["URL"] = "https://www.morningstar.com/etfs/arcx/colo/portfolio"
        # Write the updated record back to the database
        df.to_sql("ETF_RATIOS", conn, if_exists="append", index=False)
        logger.info("Record for GXG duplicated as COLO with updated URL.")
    else:
        logger.warning("No record found for index = 'GXG'.")