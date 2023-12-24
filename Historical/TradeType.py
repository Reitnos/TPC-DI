import os
from typing import NamedTuple
from sqlalchemy import create_engine

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


# Define the TradeType named tuple
class TradeType(NamedTuple):
    TT_ID: str
    TT_NAME: str
    TT_IS_SELL: int
    TT_IS_MRKT: int

def extract(file_path: str) -> list[TradeType]:
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return [TradeType(*line.strip().split("|")) for line in lines if line]


def create_trade_type_table():
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS public.tradetype (
        tt_id CHAR(3),
        tt_name CHAR(12),
        tt_is_sell NUMERIC(1),
        tt_is_mrkt NUMERIC(1),
        PRIMARY KEY (tt_id)
    );
    """
    with engine.connect() as connection:
        connection.execute(create_table_stmt)

def load(rows: list[TradeType]):
    create_trade_type_table()

    with engine.connect() as connection:
        for row in rows:
            insert_stmt = f"INSERT INTO tradetype VALUES ({', '.join(map(repr, row))});"
            connection.execute(insert_stmt)

def run_etl(file_path: str):
    raw_rows = extract(file_path)
    load(raw_rows)

if __name__ == "__main__":
    file_path = 'TradeType.txt'  # Replace with your file path
    run_etl(file_path)