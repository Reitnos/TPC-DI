import os
from typing import NamedTuple, Optional, List, Dict, Tuple
from sqlalchemy import create_engine
import datetime

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


class WatchRecord(NamedTuple):
    SK_CustomerID: int  # Customer associated with watch list
    SK_SecurityID: int  # Security listed on watch list
    SK_DateID_DatePlaced: int  # Date the watch list item was added
    SK_DateID_DateRemoved: Optional[int]  # Date the watch list item was removed, can be NULL so should be Optional
    BatchID: int  # Batch ID when this record was inserted

def get_sk_customer_id(w_c_id: str, w_dts: datetime.date) -> int:
    # SQL to get the SK_CustomerID based on CustomerID and Date
    query = """
    SELECT SK_CustomerID
    FROM public.DimCustomer
    WHERE CustomerID = %s
      AND EffectiveDate <= %s
      AND (EndDate IS NULL OR EndDate > %s)
      AND IsCurrent = TRUE;
    """

    # Execute the query and fetch the result
    with engine.connect() as connection:
        result = connection.execute(query, (w_c_id, w_dts, w_dts))
        sk_customer_id = result.scalar()

    return sk_customer_id if sk_customer_id is not None else -1

def get_sk_security_id(w_s_symb: str, w_dts: datetime.date) -> int:
    query = """
    SELECT SK_SecurityID
    FROM DimSecurity
    WHERE Symbol = %s
      AND EffectiveDate <= %s
      AND (EndDate IS NULL OR EndDate > %s)
      AND IsCurrent = TRUE;
    """
    with engine.connect() as connection:
        result = connection.execute(query, (w_s_symb, w_dts, w_dts))
        sk_security_id = result.scalar()

    return sk_security_id if sk_security_id is not None else -1

def update_security_removed(w_c_id: str, w_s_symb: str, w_dts: datetime.date, indexed_records: Dict[Tuple[int, int], WatchRecord]):
    sk_date_id_date_removed = get_sk_date_id(w_dts)
    sk_customer_id = get_sk_customer_id(w_c_id, w_dts)
    sk_security_id = get_sk_security_id(w_s_symb, w_dts)

    # Create the key for accessing the record
    record_key = (sk_customer_id, sk_security_id)

    # Check if the record exists in the indexed records
    if record_key in indexed_records:
        record = indexed_records[record_key]

        # Check if SK_DateID_DateRemoved is None before updating
        if record.SK_DateID_DateRemoved is None:
            # Update the SK_DateID_DateRemoved field
            updated_record = record._replace(SK_DateID_DateRemoved=sk_date_id_date_removed)
            indexed_records[record_key] = updated_record



    # Construct the SQL update statement
    #update_query = """
    #UPDATE FactWatches
    #SET SK_DateID_DateRemoved = %s
    #WHERE SK_CustomerID = %s AND SK_SecurityID = %s AND SK_DateID_DateRemoved IS NULL;
    #"""

    # Execute the update statement
    #with engine.connect() as connection:
        #connection.execute(update_query, (sk_date_id_date_removed, sk_customer_id, sk_security_id))

def get_sk_date_id(w_dts: datetime.date) -> int:
    query = """
    SELECT SK_DateID
    FROM DimDate
    WHERE DateValue = %s;
    """
    with engine.connect() as connection:
        result = connection.execute(query, (w_dts,))
        sk_date_id = result.scalar()

    return sk_date_id if sk_date_id is not None else -1

def extract(file_path: str) -> Dict[Tuple[int, int], WatchRecord]:
    indexed_records = {}
    with open(file_path, 'r') as file:
        for line in file:
            # Split the line into fields based on the vertical bar separator
            fields = line.strip().split('|')
            w_c_id = fields[0]
            w_dts = datetime.datetime.strptime(fields[2], '%Y-%m-%d %H:%M:%S').date()
            w_s_symb = fields[1]

            if fields[3] == 'ACTV':
                # Retrieve SK_CustomerID
                sk_customer_id = get_sk_customer_id(w_c_id, w_dts)

                # Retrieve SK_SecurityID
                sk_security_id = get_sk_security_id(w_s_symb, w_dts)

                sk_dateid_dateplaced = get_sk_date_id(w_dts)

                record = WatchRecord(SK_CustomerID=sk_customer_id, SK_SecurityID=sk_security_id,
                                     SK_DateID_DatePlaced=sk_dateid_dateplaced, SK_DateID_DateRemoved=None,
                                     BatchID=1)

                indexed_records[(sk_customer_id, sk_security_id)] = record
                # load for each line seperately !!!
                #load_individual_record(record)

            if fields[3] == 'CNCL':
                update_security_removed(w_c_id, w_s_symb, w_dts, indexed_records)
    return indexed_records


def create_factwatches_table():
    # Define SQL statement for table creation
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS FactWatches (
        SK_CustomerID INTEGER NOT NULL,
        SK_SecurityID INTEGER NOT NULL,
        SK_DateID_DatePlaced INTEGER NOT NULL,
        SK_DateID_DateRemoved INTEGER,
        BatchID INTEGER NOT NULL
    );
    """
    # Execute the SQL statement to create the table
    engine.execute(create_table_stmt)


def load(indexed_records: dict):
    with engine.connect() as connection:
        # Begin a transaction
        with connection.begin():
            # Prepare the tuples for each record
            record_tuples = [
                (record.SK_CustomerID, record.SK_SecurityID, record.SK_DateID_DatePlaced,
                 record.SK_DateID_DateRemoved if record.SK_DateID_DateRemoved is not None else None,
                 record.BatchID)
                for record in indexed_records.values()
            ]

            # Construct the multi-row insert statement
            insert_stmt = f"""
                INSERT INTO FactWatches (SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced, SK_DateID_DateRemoved, BatchID)
                VALUES {str(tuple(record_tuples))};
                """

            # Execute the multi-row insert statement
            connection.execute(insert_stmt)


def run_etl(file_path: str):
    create_factwatches_table()
    raw_rows = extract(file_path)
    load(raw_rows)


if __name__ == "__main__":
    file_path = 'WatchHistory.txt'  # Replace with your file path
    run_etl(file_path)