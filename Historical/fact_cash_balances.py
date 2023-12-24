from typing import NamedTuple
from datetime import datetime
from collections import defaultdict

from date import parse_date_file

# Define CashTransaction record data class
class CashTransactionRecord(NamedTuple):
    CT_CA_ID: int
    CT_DTS: datetime
    CT_AMT: float
    CT_NAME: str

# Define DimAccount record data class
class DimAccountRecord(NamedTuple):
    AccountID: int
    SK_CustomerID: int
    SK_AccountID: int
    EffectiveDate: datetime
    EndDate: datetime

# Define DimDate record data class
class DimDateRecord(NamedTuple):
    DateValue: str
    SK_DateID: int

# Define FactCashBalances record data class
class FactCashBalancesRecordParsed(NamedTuple):
    SK_CustomerID: int
    SK_AccountID: int
    SK_DateID: int
    Cash: float
    BatchID: int

# Define the calculate_cash_balances function with typing annotations
def calculate_cash_balances(
    cash_transactions: list[CashTransactionRecord],
    dim_accounts: list[DimAccountRecord],
    dim_dates: list[DimDateRecord]
) -> list[FactCashBalancesRecordParsed]:
    fact_cash_balances: list[FactCashBalancesRecordParsed] = []

    # Create dictionaries to store the net Cash amount for each account on a given day
    net_cash_amounts: defaultdict[int, dict[int, float]] = defaultdict(dict)
    sk_date_ids: dict[str, int] = {}

    for date_record in dim_dates:
        # Use the DateValue as the key to map to SK_DateID for quick lookups
        sk_date_ids[date_record.DateValue] = date_record.SK_DateID

    for cash_transaction in cash_transactions:
        # Filter DimAccount records based on CT_CA_ID, CT_DTS, EffectiveDate, and EndDate
        relevant_account = next((
            account for account in dim_accounts
            if account.AccountID == cash_transaction.CT_CA_ID
            and account.EffectiveDate <= cash_transaction.CT_DTS <= account.EndDate
        ), None)

        if not relevant_account:
            continue

        # Get SK_CustomerID and SK_AccountID from DimAccount
        sk_customer_id = relevant_account.SK_CustomerID
        sk_account_id = relevant_account.SK_AccountID

        # Extract date portion from CT_DTS
        ct_date = cash_transaction.CT_DTS.date()
        ct_date_str = ct_date.strftime("%Y-%m-%d")

        # Find SK_DateID from pre-loaded dictionary
        sk_date_id = sk_date_ids.get(ct_date_str)

        if sk_date_id is None:
            # Handle the case when the date is not found (you may need to adjust this based on your requirement)
            continue

        # Calculate Cash by summing CT_AMT with the net Cash amount for this account on this day
        net_cash_amount = net_cash_amounts[sk_account_id].get(sk_date_id, 0)
        net_cash_amount += cash_transaction.CT_AMT

        # Update net Cash amount for the account on this day
        net_cash_amounts[sk_account_id][sk_date_id] = net_cash_amount

    # Create FactCashBalancesRecord for each account and day with changes
    for sk_account_id, date_amounts in net_cash_amounts.items():
        for sk_date_id, cash in date_amounts.items():
            batch_id = 1

            # Create FactCashBalancesRecord
            fact_cash_balance = FactCashBalancesRecordParsed(
                SK_CustomerID=sk_customer_id,
                SK_AccountID=sk_account_id,
                SK_DateID=sk_date_id,
                Cash=cash,
                BatchID=batch_id
            )

            # Append the record to the result
            fact_cash_balances.append(fact_cash_balance)

    return fact_cash_balances

class CashTransactionRecord(NamedTuple):
    CT_CA_ID: int
    CT_DTS: datetime
    CT_AMT: float
    CT_NAME: str

def parse_cash_transaction_file(file_path: str) -> list[CashTransactionRecord]:
    cash_transactions = []

    with open(file_path) as file:
        for line in file:
            # Split the line into fields using '|'
            fields = line.strip().split('|')

            # Unpack the fields into variables
            CT_CA_ID, CT_DTS, CT_AMT, CT_NAME = fields

            # Convert string values to appropriate types
            CT_CA_ID = int(CT_CA_ID)
            CT_DTS = datetime.strptime(CT_DTS, "%Y-%m-%d %H:%M:%S")
            CT_AMT = float(CT_AMT)

            # Create CashTransactionRecord instance and append to the list
            cash_transaction = CashTransactionRecord(
                CT_CA_ID=CT_CA_ID,
                CT_DTS=CT_DTS,
                CT_AMT=CT_AMT,
                CT_NAME=CT_NAME
            )
            cash_transactions.append(cash_transaction)

    return cash_transactions

# Example usage:
cash_transactions = parse_cash_transaction_file('data/data/CashTransaction.txt')

# Example usage:
# cash_transactions = [...]  # Replace with your actual cash transaction data
# dim_accounts = [...]  # Replace with your actual DimAccount data
dim_dates = parse_date_file('data/data/Date.txt')  # Replace with your actual DimDate data
synthetic_data = [
    DimAccountRecord(
        SK_AccountID=2919,
        AccountID=1001,
        SK_CustomerID=3001,
        EffectiveDate=datetime(2023, 1, 1),
        EndDate=datetime(2023, 12, 31)
    ),
    DimAccountRecord(
        SK_AccountID=2,
        AccountID=2181,
        SK_CustomerID=3002,
        EffectiveDate=datetime(2023, 2, 1),
        EndDate=datetime(2023, 11, 30)
    ),
]
fact_cash_balances = calculate_cash_balances(cash_transactions, synthetic_data, dim_dates)
print(len(fact_cash_balances))
