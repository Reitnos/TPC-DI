from dataclasses import dataclass
from enum import Enum

# Define the ActionType values using StrEnum
class ActionType(str, Enum):
    NEW = 'NEW'
    ADDACCT = 'ADDACCT'
    UPDACCT = 'UPDACCT'
    UPDCUST = 'UPDCUST'
    CLOSEACCT = 'CLOSEACCT'
    INACT = 'INACT'

@dataclass
class DimAccountRecord:
    ActionType: ActionType
    ActionTS: str
    CA_ID: int
    CA_NAME: str
    CA_TAX_ST: int
    CA_B_ID: int
    C_ID: int

@dataclass
class DimAccount:
    SK_AccountID: int = None
    AccountID: int = None
    SK_BrokerID: int = None
    SK_CustomerID: int = None
    Status: str = None
    AccountDesc: str = None
    TaxStatus: int = None
    IsCurrent: bool = None
    BatchID: int = None
    EffectiveDate: str = None
    EndDate: str = None

def process_dim_account_records(records: list[DimAccountRecord]) -> list[DimAccount]:
    dim_accounts = []

    for record in records:
        dim_account = DimAccount()

        # Common fields for all ActionTypes
        dim_account.AccountID = record.CA_ID
        dim_account.AccountDesc = record.CA_NAME
        dim_account.TaxStatus = record.CA_TAX_ST
        dim_account.SK_BrokerID = record.CA_B_ID
        dim_account.SK_CustomerID = record.C_ID

        # ActionType specific processing
        if record.ActionType in (ActionType.NEW, ActionType.ADDACCT):
            dim_account.Status = 'ACTIVE'
        elif record.ActionType == ActionType.UPDACCT:
            # Process fields that exist in the Source Data
            # Fields not present retain their values from the current record in DimAccount
            pass
        elif record.ActionType == ActionType.CLOSEACCT:
            dim_account.Status = 'INACTIVE'
        elif record.ActionType in (ActionType.UPDCUST, ActionType.INACT):
            dim_account.SK_CustomerID = get_updated_customer_id(record.C_ID, record.ActionTS)
            dim_account.Status = 'INACTIVE' if record.ActionType == ActionType.INACT else 'ACTIVE'
            dim_account.IsCurrent, dim_account.EffectiveDate, dim_account.EndDate = get_history_tracking_dates(record.ActionTS)
            dim_account.BatchID = get_batch_id()

        dim_accounts.append(dim_account)

    return dim_accounts

def get_updated_customer_id(customer_id: int, action_timestamp: str) -> int:
    # Logic to obtain the updated customer ID based on the action timestamp
    pass

def get_history_tracking_dates(action_timestamp: str) -> tuple:
    # Logic to determine IsCurrent, EffectiveDate, and EndDate based on action timestamp
    # Using defaults from section 4.4.1
    is_current = True
    effective_date = action_timestamp
    end_date = '9999-12-31'
    return is_current, effective_date, end_date

def get_batch_id() -> int:
    return 1


from typing import NamedTuple, list, Optional, Tuple
import xml.etree.ElementTree as ET

class PhoneNumber(NamedTuple):
    C_CTRY_CODE: str
    C_AREA_CODE: str
    C_LOCAL: str
    C_EXT: Optional[str] = None

class Address(NamedTuple):
    C_ADLINE1: str
    C_ADLINE2: Optional[str] = None
    C_ZIPCODE: str
    C_CITY: str
    C_STATE_PROV: str
    C_CTRY: str

class ContactInfo(NamedTuple):
    C_PRIM_EMAIL: Optional[str] = None
    C_ALT_EMAIL: Optional[str] = None
    C_PHONE_1: Optional[PhoneNumber] = None
    C_PHONE_2: Optional[PhoneNumber] = None
    C_PHONE_3: Optional[PhoneNumber] = None

class TaxInfo(NamedTuple):
    C_LCL_TX_ID: str
    C_NAT_TX_ID: str

class Name(NamedTuple):
    C_L_NAME: str
    C_F_NAME: str
    C_M_NAME: Optional[str] = None

class Account(NamedTuple):
    CA_ID: str
    CA_TAX_ST: str
    CA_B_ID: Optional[str] = None
    CA_NAME: Optional[str] = None

class Customer(NamedTuple):
    C_ID: str
    C_TAX_ID: str
    C_GNDR: str
    C_TIER: int
    C_DOB: str
    Name: Optional[Name] = None
    Address: Optional[Address] = None
    ContactInfo: Optional[ContactInfo] = None
    TaxInfo: Optional[TaxInfo] = None
    Account: list[Account] = []

def parse_customer_mgmt(xml_path: str) -> tuple[list[Customer], list[Account]]:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    customers: list[Customer] = []
    accounts: list[Account] = []

    for action_elem in root.findall('.//Action'):
        action_type = action_elem.get('ActionType')
        action_ts = action_elem.get('ActionTS')

        customer_elem = action_elem.find('./Customer')
        if customer_elem is not None:
            customer = Customer(
                C_ID=customer_elem.get('C_ID'),
                C_TAX_ID=customer_elem.get('C_TAX_ID'),
                C_GNDR=customer_elem.get('C_GNDR'),
                C_TIER=int(customer_elem.get('C_TIER')),
                C_DOB=customer_elem.get('C_DOB')
            )

            customer.Name = Name(
                C_L_NAME=customer_elem.findtext('./Name/C_L_NAME'),
                C_F_NAME=customer_elem.findtext('./Name/C_F_NAME'),
                C_M_NAME=customer_elem.findtext('./Name/C_M_NAME')
            )

            customer.Address = Address(
                C_ADLINE1=customer_elem.findtext('./Address/C_ADLINE1'),
                C_ADLINE2=customer_elem.findtext('./Address/C_ADLINE2'),
                C_ZIPCODE=customer_elem.findtext('./Address/C_ZIPCODE'),
                C_CITY=customer_elem.findtext('./Address/C_CITY'),
                C_STATE_PROV=customer_elem.findtext('./Address/C_STATE_PROV'),
                C_CTRY=customer_elem.findtext('./Address/C_CTRY')
            )

            customer.ContactInfo = ContactInfo(
                C_PRIM_EMAIL=customer_elem.findtext('./ContactInfo/C_PRIM_EMAIL'),
                C_ALT_EMAIL=customer_elem.findtext('./ContactInfo/C_ALT_EMAIL'),
                C_PHONE_1=PhoneNumber(
                    C_CTRY_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_1/C_CTRY_CODE'),
                    C_AREA_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_1/C_AREA_CODE'),
                    C_LOCAL=customer_elem.findtext('./ContactInfo/C_PHONE_1/C_LOCAL'),
                    C_EXT=customer_elem.findtext('./ContactInfo/C_PHONE_1/C_EXT')
                ),
                C_PHONE_2=PhoneNumber(
                    C_CTRY_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_2/C_CTRY_CODE'),
                    C_AREA_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_2/C_AREA_CODE'),
                    C_LOCAL=customer_elem.findtext('./ContactInfo/C_PHONE_2/C_LOCAL'),
                    C_EXT=customer_elem.findtext('./ContactInfo/C_PHONE_2/C_EXT')
                ),
                C_PHONE_3=PhoneNumber(
                    C_CTRY_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_3/C_CTRY_CODE'),
                    C_AREA_CODE=customer_elem.findtext('./ContactInfo/C_PHONE_3/C_AREA_CODE'),
                    C_LOCAL=customer_elem.findtext('./ContactInfo/C_PHONE_3/C_LOCAL'),
                    C_EXT=customer_elem.findtext('./ContactInfo/C_PHONE_3/C_EXT')
                )
            )

            customer.TaxInfo = TaxInfo(
                C_LCL_TX_ID=customer_elem.findtext('./TaxInfo/C_LCL_TX_ID'),
                C_NAT_TX_ID=customer_elem.findtext('./TaxInfo/C_NAT_TX_ID')
            )

            accounts_elem = customer_elem.findall('./Account')
            for account_elem in accounts_elem:
                account = Account(
                    CA_ID=account_elem.get('CA_ID'),
                    CA_TAX_ST=account_elem.get('CA_TAX_ST'),
                    CA_B_ID=account_elem.findtext('./CA_B_ID'),
                    CA_NAME=account_elem.findtext('./CA_NAME')
                )
                customer.Account.append(account)
                accounts.append(account)

            customers.append(customer)

    return customers, accounts

# Example usage:
# customers, accounts = parse_customer_mgmt('CustomerMgmt.xml')
# for customer in customers:
#     print(f"Customer ID: {customer.C_ID}, Accounts: {len(customer.Account)}")
#     for account in customer.Account:
#         print(f"  Account ID: {account.CA_ID}, Tax Status: {account.CA_TAX_ST}")


# Example usage:
# dim_account_records = [DimAccountRecord(ActionType.NEW, "2022-01-01", 1, "Account1", 1, 1)]
# dim_accounts = process_dim_account_records(dim_account_records)
# print(dim_accounts)
