import os
from typing import NamedTuple, List
from sqlalchemy import create_engine, text

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


class FactHolding(NamedTuple):  # Data type for FactHoldings table
    TradeID: str
    CurrentTradeID: str
    SK_CustomerID: str
    SK_AccountID: str
    SK_SecurityID: str
    SK_CompanyID: str
    SK_DateID: str
    SK_TimeID: str
    CurrentPrice: float
    CurrentHolding: int
    BatchID: int


def get_dim_trade_data(trade_id: str) -> dict:
    query_dim_trade = text("""
        SELECT SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, TradePrice AS CurrentPrice, SK_CloseDateID, SK_CloseTimeID
        FROM DimTrade
        WHERE TradeID = :trade_id
    """)

    with engine.connect() as connection:
        result = connection.execute(query_dim_trade, {"trade_id": trade_id}).fetchone()

    # Convert the result to a dictionary if not None
    return dict(result) if result else {'sk_customerid':1, 'sk_accountid':1, 'sk_securityid':1, 'sk_companyid':1, 'sk_closedateid':1, 'sk_closetimeid':1, 'currentprice':1}  #should be none

def extract(file_path: str) -> List[FactHolding]:
    with open(file_path, 'r') as file:
        lines = file.readlines()

    extracted_data = []

    for line in lines:
        if line:
            parts = line.strip().split("|")

            # Assuming that the HoldingHistory.txt file has the fields in the order as per Table 2.2.9:
            # HH_H_T_ID | HH_T_ID | HH_BEFORE_QTY | HH_AFTER_QTY

            # Retrieve the DimTrade data for the current trade_id
            trade_id = parts[0]
            dim_trade_data = get_dim_trade_data(trade_id)


            # Create a FactHolding instance
            holding = FactHolding(
                TradeID=parts[0],  # HH_T_ID
                CurrentTradeID=parts[1],  # HH_H_T_ID
                SK_CustomerID=dim_trade_data['sk_customerid'],
                SK_AccountID=dim_trade_data['sk_accountid'],
                SK_SecurityID=dim_trade_data['sk_securityid'],
                SK_CompanyID=dim_trade_data['sk_companyid'],
                SK_DateID=dim_trade_data['sk_closedateid'],
                SK_TimeID=dim_trade_data['sk_closetimeid'],
                CurrentPrice=dim_trade_data['currentprice'],
                CurrentHolding=int(parts[3]),  # HH_AFTER_QTY
                BatchID=1
            )

            extracted_data.append(holding)

    return extracted_data


def create_fact_holdings_table():
    # SQL statement for table creation
    create_table_stmt = text("""
    CREATE TABLE IF NOT EXISTS FactHoldings (
        TradeID VARCHAR(255) NOT NULL,
        CurrentTradeID VARCHAR(255) NOT NULL,
        SK_CustomerID INTEGER NOT NULL, -- Assuming this is a foreign key, not a primary key
        SK_AccountID INTEGER NOT NULL,
        SK_SecurityID INTEGER NOT NULL,
        SK_CompanyID INTEGER NOT NULL,
        SK_DateID INTEGER NOT NULL,
        SK_TimeID INTEGER NOT NULL,
        CurrentPrice NUMERIC NOT NULL CHECK (CurrentPrice > 0),
        CurrentHolding INTEGER NOT NULL,
        BatchID INTEGER NOT NULL
    -- Add PRIMARY KEY or FOREIGN KEY constraints as necessary
);
    """)

    # Execute the statement using the engine
    with engine.connect() as conn:
        conn.execute(create_table_stmt)

def load(rows: List[FactHolding]):

    create_fact_holdings_table()
    # Define SQL statements for data insertion
    insert_stmt = text("""
    INSERT INTO FactHoldings (
        TradeID,
        CurrentTradeID,
        SK_CustomerID,
        SK_AccountID,
        SK_SecurityID,
        SK_CompanyID,
        SK_DateID,
        SK_TimeID,
        CurrentPrice,
        CurrentHolding,
        BatchID
    ) VALUES (
        :TradeID,
        :CurrentTradeID,
        :SK_CustomerID,
        :SK_AccountID,
        :SK_SecurityID,
        :SK_CompanyID,
        :SK_DateID,
        :SK_TimeID,
        :CurrentPrice,
        :CurrentHolding,
        :BatchID
    );
    """)

    # Use the engine to connect to the database and execute the insert statement
    with engine.connect() as conn:
        # Execute the insert statement for each row
        for row in rows:
            conn.execute(insert_stmt, **row._asdict())

def run_etl(file_path: str):
    raw_rows = extract(file_path)
    load(raw_rows)


if __name__ == "__main__":
    file_path = 'HoldingHistory.txt'  # Replace with your file path
    run_etl(file_path)