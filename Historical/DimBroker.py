import os
from typing import NamedTuple
from sqlalchemy import create_engine
import sqlalchemy
from datetime import datetime
from typing import NamedTuple, Optional
import csv

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


class DimBroker(NamedTuple):
    #SK_BrokerID: int
    BrokerID: str
    ManagerID: str
    FirstName: str
    LastName: str
    MiddleInitial: Optional[str]
    Branch: str
    Office: str
    Phone: str
    IsCurrent: bool
    BatchID: int
    EffectiveDate: datetime
    EndDate: datetime

def get_earliest_date_from_dimdate() -> datetime:
    # Query to select the earliest date from the DimDate table
    query = "SELECT MIN(DateValue) AS earliest_date FROM DimDate"
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
        return result['earliest_date'] if result else datetime(1900, 1, 1)

def extract(file_path: str) -> list[DimBroker]:
    brokers = []
    earliest_date = get_earliest_date_from_dimdate()
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            # Check if the employee is a broker with job code 314
            if row and row[5] == '314':  # Assuming EmployeeJobCode is in the 7th column (index 6)
                broker = DimBroker(
                    #SK_BrokerID=None,  # This will be set by the database as SERIAL [TO CHANGE, don't understand this]
                    BrokerID=row[0],  # EmployeeID
                    ManagerID=row[1],  # ManagerID
                    FirstName=row[2],  # EmployeeFirstName
                    LastName=row[3],  # EmployeeLastName
                    MiddleInitial=row[4] if row[4] else None,  # EmployeeMI
                    Branch=row[6],  # EmployeeBranch
                    Office=row[7],  # EmployeeOffice
                    Phone=row[8],  # EmployeePhone
                    IsCurrent=True,
                    BatchID=1,  # Historical Load, BatchID = 1
                    EffectiveDate=earliest_date,  # TOCHANGE: Get earliest date from dimtable
                    EndDate=datetime(9999, 12, 31)  # Use the end date provided
                )
                brokers.append(broker)
    return brokers

def create_dim_broker_table():
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS public.dimbroker (
        sk_brokerid SERIAL PRIMARY KEY,
        brokerid VARCHAR(50) NOT NULL,
        managerid VARCHAR(50),
        firstname VARCHAR(50) NOT NULL,
        lastname VARCHAR(50) NOT NULL,
        middleinitial CHAR(1),
        branch VARCHAR(50),
        office VARCHAR(50),
        phone CHAR(14),
        iscurrent BOOLEAN NOT NULL,
        batchid INTEGER NOT NULL,
        effectivedate DATE NOT NULL,
        enddate DATE NOT NULL
    );
    """
    with engine.connect() as connection:
        connection.execute(create_table_stmt)

def transform(raw_rows: list[DimBroker]) -> list[DimBroker]:
    return raw_rows


def load(rows: list[DimBroker]):
    create_dim_broker_table()

    # Define the column names for the INSERT statement
    column_names = [
        'brokerid', 'managerid', 'firstname', 'lastname',
        'middleinitial', 'branch', 'office', 'phone',
        'iscurrent', 'batchid', 'effectivedate', 'enddate'
    ]

    # Prepare the insert statement outside the loop
    insert_stmt = sqlalchemy.text(f"""
        INSERT INTO dimbroker ({', '.join(column_names)}) 
        VALUES (:BrokerID, :ManagerID, :FirstName, :LastName, :MiddleInitial, :Branch, :Office, :Phone, :IsCurrent, :BatchID, :EffectiveDate, :EndDate)
    """)

    with engine.connect() as connection:
        for row in rows:
            # Use a dictionary to store the values for parameterized SQL
            row_dict = row._asdict()

            # Replace single quotes in string values
            for key, value in row_dict.items():
                if isinstance(value, str):
                    row_dict[key] = value.replace('\'', '\'\'')
                elif isinstance(value, datetime):
                    row_dict[key] = value.strftime('%Y-%m-%d')

            # Execute the parameterized SQL statement with the dictionary
            connection.execute(insert_stmt, **row_dict)


def run_etl(file_path: str):
    raw_rows = extract(file_path)
    transformed_rows = transform(raw_rows)
    load(transformed_rows)


if __name__ == "__main__":
    file_path = 'HR.csv'  # Replace with your file path
    run_etl(file_path)