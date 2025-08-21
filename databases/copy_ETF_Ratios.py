import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from databases.database_mysql import SQLA_read_table

DIR_OUTPUT = "D:/OneDrive/Python Scripts/Financial files/"


def main():
    dfExisting = SQLA_read_table(tablename="ETF_RATIOS")
    print(dfExisting)
    dfExisting.to_csv(
        DIR_OUTPUT + "ETF_RATIOS.csv",
    )
    print(f"ETF_RATIOS.csv saved in {DIR_OUTPUT}")


if __name__ == "__main__":
    main()
