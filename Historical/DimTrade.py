import os
from typing import NamedTuple
from sqlalchemy import create_engine, text
from typing import NamedTuple, Optional, List, Tuple, Dict
from datetime import datetime

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


class DimTradeRecord(NamedTuple):
    TradeID: str  # Trade identifier
    SK_BrokerID: int  # Surrogate key for BrokerID
    SK_CreateDateID: int  # Surrogate key for date created
    SK_CreateTimeID: int  # Surrogate key for time created
    SK_CloseDateID: int  # Surrogate key for date closed
    SK_CloseTimeID: int  # Surrogate key for time closed
    Status: str  # Trade status
    Type: str  # Trade type
    CashFlag: bool  # Is this trade a cash (1) or margin (0) trade?
    SK_SecurityID: int  # Surrogate key for SecurityID
    SK_CompanyID: int  # Surrogate key for CompanyID
    Quantity: int  # Quantity of securities traded
    BidPrice: float  # The requested unit price
    SK_CustomerID: int  # Surrogate key for CustomerID
    SK_AccountID: int  # Surrogate key for AccountID
    ExecutedBy: str  # Name of person executing the trade
    TradePrice: float  # Unit price at which the security was traded
    Fee: float  # Fee charged for placing this trade request
    Commission: float  # Commission earned on this trade
    Tax: float  # Amount of tax due on this trade
    BatchID: int  # Batch ID when this record was inserted


def parse_boolean(value: str) -> bool:
    return value == '1'


def parse_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None


def get_status_name(t_st_id):
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT ST_NAME FROM StatusType WHERE ST_ID = %s", (t_st_id,)
        ).fetchone()
    return result[0] if result else 'dum'

def get_trade_type_name(t_tt_id: str) -> Optional[str]:
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT tt_name FROM tradetype WHERE tt_id = %s",
            (t_tt_id,)
        ).fetchone()
    return result['tt_name'].strip() if result else None

def get_security_info(t_s_symb: str, th_dts: datetime) -> (Optional[int], Optional[int]):
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT SK_SecurityID, SK_CompanyID FROM DimSecurity WHERE Symbol = %s AND %s BETWEEN EffectiveDate AND EndDate",
            (t_s_symb, th_dts)
        ).fetchone()
    return (result['sk_securityid'], result['sk_companyid']) if result else (None, None)

def get_account_info(t_ca_id, th_dts):
    with engine.connect() as connection:
        result = connection.execute(
            """
            SELECT SK_AccountID, SK_CustomerID, SK_BrokerID 
            FROM DimAccount 
            WHERE AccountID = %s AND %s BETWEEN EffectiveDate AND EndDate
            AND IsCurrent = TRUE
            """,
            (t_ca_id, th_dts)
        ).fetchone()
    return (result['sk_accountid'], result['sk_customerid'], result['sk_brokerid']) if result else (None, None, None)

def get_sk_dateid(date_obj: datetime.date) -> Optional[int]:
    # Parse the datetime string to get the date part

    # Query the DateDim table to get the surrogate key for the date part
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT SK_DateID FROM DimDate WHERE DateValue = %s",
            (date_obj,)
        ).fetchone()
    return result['sk_dateid'] if result else None

def get_sk_timeid(time_component: datetime.time) -> Optional[int]:
    # Format the time to match the TimeValue field in the DimTime table
    formatted_time = time_component.strftime('%H:%M:%S')

    # Query the DimTime table to get the surrogate key for the time part
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT SK_TimeID FROM DimTime WHERE TimeValue = %s",
            (formatted_time,)
        ).fetchone()

    return result['SK_TimeID'] if result else None

def get_status_name_from_code(status_code):
    # SQL query to get ST_NAME from StatusType table where the ST_ID matches the status_code
    query = text("""
        SELECT ST_NAME
        FROM StatusType
        WHERE ST_ID = :status_code
    """)

    with engine.connect() as conn:
        result = conn.execute(query, status_code=status_code)
        status_name = result.scalar()

    return status_name

def parse_t_dts(t_dts_str):
    # Parse the T_DTS string into a datetime object
    t_dts = datetime.strptime(t_dts_str, '%Y-%m-%d %H:%M:%S')

    # Extract the date and time components
    date_component = t_dts.date()  # This will give you a date object
    time_component = t_dts.time()  # This will give you a time object

    return date_component, time_component

def parse_t_dts_single(t_dts_str: str) -> Tuple[datetime.date, datetime.time]:
    # Parse the T_DTS string into a datetime object
    t_dts = datetime.strptime(t_dts_str, '%Y-%m-%d %H:%M:%S')
    return t_dts.date(), t_dts.time()
def load_history_into_dict(history_path: str) -> dict:
    history_dict = {}
    with open(history_path, 'r') as file:
        for line in file:
            if line.strip():  # Ensure the line is not empty
                fields = line.strip().split("|")
                trade_id = fields[0]
                th_dts = parse_t_dts_single(fields[1])
                # Using (trade_id, th_dts) as the key
                history_dict[(trade_id, th_dts)] = fields[2]
    return history_dict


def extract(file_path_trade: str,file_path_tradehist: str) -> List[DimTradeRecord]:
    with open(file_path, 'r') as file:
        lines = file.readlines()

    count = 0
    existing_trade_ids = set()
    history_dict = load_history_into_dict(file_path_tradehist)
    extracted_records = []
    for line in lines:
        print(count)
        count+=1
        if line.strip():  # Ensure the line is not empty
            fields = line.strip().split("|")

            # Extract the status, type identifiers, and other necessary fields
            trade_id = fields[0]
            trade_datetime = parse_t_dts_single(fields[1])

            # Look up the corresponding history using (trade_id, trade_datetime)
            history_field = history_dict.get((trade_id, trade_datetime))

             # Skip the first two fields which are trade_id and datetime

            # Parse to boolean, True if '1' (cash) and False if '0' (margin)
            cash_flag = True if fields[4] == '1' else False
            quantity = int(fields[6])
            bid_price = float(fields[7])
            executed_by = fields[9]
            trade_price = float(fields[10]) if fields[10] else None
            fee = float(fields[11]) if fields[11] else None
            commission = float(fields[12]) if fields[12] else None
            tax = float(fields[13]) if fields[13] else None

            fields = line.strip().split("|")
            th_st_id = history_field
            t_tt_id = fields[3]
            t_dts_str = fields[1]
            date_component, time_component = parse_t_dts(t_dts_str)
            th_dts = datetime.combine(date_component, time_component)

            status = get_status_name(th_st_id)
            trade_type = get_trade_type_name(t_tt_id)

            t_s_symb = fields[5]
            sk_security_id, sk_company_id = get_security_info(t_s_symb, th_dts)

            t_ca_id = fields[8]
            sk_account_id, sk_customer_id, sk_broker_id = get_account_info(t_ca_id, th_dts)


            new_entry = trade_id not in existing_trade_ids
            if new_entry:
                existing_trade_ids.add(trade_id)

            # Determine if we should set SK_CreateDateID and SK_CreateTimeID
            if th_st_id in ["SBMT", "PNDG"] or t_tt_id in ["TMB", "TMS"]:
                time_component = time_component.replace(microsecond=0)

                # Get the surrogate keys for the date and time components
                sk_create_date_id = get_sk_dateid(date_component)
                sk_create_time_id = get_sk_timeid(time_component)

                if new_entry:
                    sk_close_date_id = None
                    sk_close_time_id = None
                else:
                    sk_close_date_id = None
                    sk_close_time_id = None


            if th_st_id in ["CMPT", "CNCL"]:
                time_component = time_component.replace(microsecond=0)

                sk_close_date_id = get_sk_dateid(t_dts_str)
                sk_close_time_id = get_sk_timeid(time_component)

                if new_entry:
                    sk_create_date_id = None
                    sk_create_time_id = None
                else:
                    sk_create_date_id = None
                    sk_create_time_id = None

                # Create the DimTradeRecord instance
            record = DimTradeRecord(
                    TradeID=trade_id,
                    SK_BrokerID=None,
                    SK_CreateDateID=sk_create_date_id,
                    SK_CreateTimeID=sk_create_time_id,
                    SK_CloseDateID=sk_close_date_id,
                    SK_CloseTimeID=sk_close_time_id,
                    Status=status,
                    Type=trade_type,
                    CashFlag=cash_flag,
                    SK_SecurityID=sk_security_id,
                    SK_CompanyID=sk_company_id,
                    Quantity=quantity,
                    BidPrice=bid_price,
                    SK_CustomerID=sk_customer_id,
                    SK_AccountID=sk_account_id,
                    ExecutedBy=executed_by,
                    TradePrice=trade_price,
                    Fee=fee,
                    Commission=commission,
                    Tax=tax,
                    BatchID=1
            )
            extracted_records.append(record)
            if count > 100:
                break
    return extracted_records

def create_dim_trade_table():
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS public.dimtrade (
        TradeID VARCHAR(50) NOT NULL,
        SK_BrokerID INTEGER NOT NULL,
        SK_CreateDateID INTEGER NOT NULL,
        SK_CreateTimeID INTEGER NOT NULL,
        SK_CloseDateID INTEGER,
        SK_CloseTimeID INTEGER,
        Status CHAR(10) NOT NULL,
        TradeType CHAR(12) NOT NULL,
        CashFlag BOOLEAN NOT NULL,
        SK_SecurityID INTEGER NOT NULL,
        SK_CompanyID INTEGER NOT NULL,
        Quantity NUMERIC(6,0) NOT NULL,
        BidPrice NUMERIC(8,2) NOT NULL,
        SK_CustomerID INTEGER NOT NULL,
        SK_AccountID INTEGER NOT NULL,
        ExecutedBy CHAR(64) NOT NULL,
        TradePrice NUMERIC(8,2),
        Fee NUMERIC(10,2),
        Commission NUMERIC(10,2),
        Tax NUMERIC(10,2),
        BatchID INTEGER,
        PRIMARY KEY (TradeID)  -- Assuming TradeID is the unique identifier for the table
    );
    """
    with engine.connect() as connection:
        connection.execute(create_table_stmt)


def load(rows: list[DimTradeRecord]):
    create_dim_trade_table()

    # Define the SQL statement for bulk insertion
    insert_stmt = """
        INSERT INTO dimtrade (
            TradeID, SK_BrokerID, SK_CreateDateID, SK_CreateTimeID, SK_CloseDateID,
            SK_CloseTimeID, Status, TradeType, CashFlag, SK_SecurityID, SK_CompanyID, 
            Quantity, BidPrice, SK_CustomerID, SK_AccountID, ExecutedBy, 
            TradePrice, Fee, Commission, Tax, BatchID
        ) VALUES (
            %(TradeID)s, %(SK_BrokerID)s, %(SK_CreateDateID)s, %(SK_CreateTimeID)s, %(SK_CloseDateID)s,
            %(SK_CloseTimeID)s, %(Status)s, %(Type)s, %(CashFlag)s, %(SK_SecurityID)s, %(SK_CompanyID)s, 
            %(Quantity)s, %(BidPrice)s, %(SK_CustomerID)s, %(SK_AccountID)s, %(ExecutedBy)s, 
            %(TradePrice)s, %(Fee)s, %(Commission)s, %(Tax)s, %(BatchID)s
        )
    """

    # Convert the list of DimTradeRecord instances to a list of dictionaries
    data_to_insert = [row._asdict() for row in rows]

    # Execute the bulk insert
    with engine.connect() as connection:
        connection.execute(insert_stmt, data_to_insert)


def run_etl(file_path: str):
    raw_rows = extract('Trade.txt', 'TradeHistory.txt')
    load(raw_rows)


if __name__ == "__main__":
    file_path = 'Trade.txt'  # Replace with your file path
    run_etl(file_path)