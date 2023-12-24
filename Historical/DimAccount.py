from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
import datetime
from sqlalchemy import create_engine, text
from typing import NamedTuple, List, Optional, Tuple
import xml.etree.ElementTree as ET
from dataclasses import dataclass, fields

# Database connection parameters
dbname = 'tpc-di'
user = 'postgres'
password = 'datamining'
host = 'localhost'  # localhost or the server address
port = '5433'  # default PostgreSQL port is 5432

# Establish a connection to the database
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(connection_str)


class PhoneNumber(NamedTuple):
    C_CTRY_CODE: str
    C_AREA_CODE: str
    C_LOCAL: str
    C_EXT: Optional[str] = None

class Address(NamedTuple):
    C_ADLINE1: str
    C_ZIPCODE: str
    C_CITY: str
    C_STATE_PROV: str
    C_CTRY: str
    C_ADLINE2: Optional[str] = None

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
    C_M_NAME: Optional[str]

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
    ActionType: str
    Name: Optional[Name] = None
    Address: Optional[Address] = None
    ContactInfo: Optional[ContactInfo] = None
    TaxInfo: Optional[TaxInfo] = None
    Account: list[Account] = []
# Define the ActionType values using StrEnum
class ActionType(str, Enum):
    NEW = 'NEW'
    ADDACCT = 'ADDACCT'
    UPDACCT = 'UPDACCT'
    UPDCUST = 'UPDCUST'
    CLOSEACCT = 'CLOSEACCT'
    INACT = 'INACT'
@dataclass
class RawDimCustomer:
    ActionType: str
    CustomerID: str  # Customer identifier
    TaxID: str  # Customer's tax identifier
    Status: str  # Customer status type
    LastName: str  # Customer's last name
    FirstName: str  # Customer's first name
    MiddleInitial: Optional[str]  # Customer's middle name initial
    Gender: str  # Gender of the customer
    Tier: int  # Customer tier
    DOB: str  # Customer's date of birth
    AddressLine1: str  # Address Line 1
    AddressLine2: Optional[str]  # Address Line 2
    PostalCode: str  # Zip or Postal Code
    City: str  # City
    StateProv: str  # State or Province
    Country: str  # Country
    Phone1: Optional[str]  # Phone number 1
    Phone2: Optional[str]  # Phone number 2
    Phone3: Optional[str]  # Phone number 3
    Email1: Optional[str]  # Email address 1
    Email2: Optional[str]  # Email address 2
    NationalTaxRateDesc: str  # National Tax rate description
    NationalTaxRate: float  # National Tax rate
    LocalTaxRateDesc: str  # Local Tax rate description
    LocalTaxRate: float  # Local Tax rate
    AgencyID: str  # Agency identifier
    CreditRating: int  # Credit rating
    NetWorth: float  # Net worth
    MarketingNameplate: str  # Marketing nameplate
    IsCurrent: bool  # True if this is the current record
    BatchID: int  # Batch ID when this record was inserted
    EffectiveDate: str  # Beginning of date range when this record was the current record
    EndDate: str  # Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31

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

def lookup_tax_rate(tax_id: str):
    """
    Generalized function to lookup tax rate information.

    :param tax_id: The tax ID to lookup.
    :return: A tuple containing the tax rate description and the tax rate.
    """
    if tax_id is None:
        return None, None

    tax_rate_query = text("""
        SELECT TX_NAME, TX_RATE
        FROM TaxRate
        WHERE TX_ID = :tax_id
    """)

    with engine.connect() as connection:
        result = connection.execute(tax_rate_query, tax_id=tax_id).first()

    if result:
        return result['TX_NAME'], result['TX_RATE']
    else:
        return None, None

def lookup_prospect_data(last_name: str, first_name: str, address_line1: str, address_line2: str,
                         postal_code: str):
    """
    Function to lookup prospect data for a given customer ID and match against the provided demographic data.

    :param customer_id: The customer ID to lookup.
    :param last_name: The last name to match.
    :param first_name: The first name to match.
    :param address_line1: The address line 1 to match.
    :param address_line2: The address line 2 to match.
    :param postal_code: The postal code to match.
    :return: A tuple containing the AgencyID, CreditRating, NetWorth, and MarketingNameplate, or None for each if not found or if no match.
    """
    # Placeholder for the actual SQL query you would use
    prospect_query = text("""
        SELECT AgencyID, CreditRating, NetWorth, MarketingNameplate
        FROM Prospect
        WHERE UPPER(LastName) = UPPER(:last_name)
        AND UPPER(FirstName) = UPPER(:first_name)
        AND UPPER(AddressLine1) = UPPER(:address_line1)
        AND (UPPER(AddressLine2) = UPPER(:address_line2) OR AddressLine2 IS NULL)
        AND UPPER(PostalCode) = UPPER(:postal_code)
        AND IsCustomer = TRUE
        ORDER BY SK_UpdateDateID DESC
        LIMIT 1
    """)

    # Define the parameters for the SQL query
    params = {
        'last_name': last_name,
        'first_name': first_name,
        'address_line1': address_line1,
        'address_line2': address_line2 or '',  # If address_line2 is None, pass an empty string
        'postal_code': postal_code
    }

    with engine.connect() as connection:
        result = connection.execute(prospect_query, **params).first()

    if result:
        return result['AgencyID'], result['CreditRating'], result['NetWorth'], result['MarketingNameplate']
    else:
        return None, None, None, None

def process_dim_customer_records(records: list[Customer]) -> list[RawDimCustomer]:

    customers = []
    count = 0
    for i, record in enumerate(records):


        ActionType = record.ActionType
        # BULLET POINT 1
        # Extracting attributes directly from Customer element
        customer_id = record.C_ID
        tax_id = record.CA_TAX_ID
        # Extracting and safely converting Tier
        tier = record.C_TIER
        try:
            tier = int(tier) if tier is not None else None
        except ValueError:
            tier = None
        dob = record.C_DOB

        # Extracting values from the Name sub-element
        name_elem = record.Name
        last_name = record.Name.C_L_NAME if name_elem is not None and record.Name.C_L_NAME is not None else None
        first_name = name_elem.C_F_NAME if name_elem is not None else None
        middle_initial = name_elem.C_M_NAME if name_elem is not None else None

        # Extracting values from the ContactInfo sub-element
        contact_info_elem = record.ContactInfo
        email1 = contact_info_elem.C_PRIM_EMAIL if contact_info_elem is not None else None
        email2 = contact_info_elem.C_ALT_EMAIL if contact_info_elem is not None else None

        # BULLET POINT 2
        # Extracting and processing the Gender attribute
        gender = record.C_GNDR.upper() if record.C_GNDR is not None else 'U'

        # Ensuring gender is one of the specified options ('M', 'F', or 'U')
        if gender not in ('M', 'F'):
            gender = 'U'

        # BULLET POINT 3

        # Extracting values from the Address sub-element
        # Extracting values from the Address instance
        address_elem = record.Address
        address_line1 = address_elem.C_ADLINE1 if address_elem is not None else None
        address_line2 = address_elem.C_ADLINE2 if address_elem is not None else None
        postal_code = address_elem.C_ZIPCODE if address_elem is not None else None
        city = address_elem.C_CITY if address_elem is not None else None
        state_prov = address_elem.C_STATE_PROV if address_elem is not None else None
        country = address_elem.C_CTRY if address_elem is not None else None

        # BULLET POINT 4
        # Function to format phone numbers
        def format_phone_number(phone: PhoneNumber):
            if phone is None:
                return None

            country_code = phone.C_CTRY_CODE
            area_code = phone.C_AREA_CODE
            local = phone.C_LOCAL
            ext = phone.C_EXT

            # Apply formatting based on the presence of components
            if country_code and area_code and local:
                phone_formatted = f"+{country_code} ({area_code}) {local}"
            elif area_code and local:
                phone_formatted = f"({area_code}) {local}"
            elif local:
                phone_formatted = local
            else:
                return None

            # Append extension if present
            if ext:
                phone_formatted += f" ext. {ext}"

            return phone_formatted

        # Assuming `record` is an instance of `Customer` with `ContactInfo` that contains `PhoneNumber` instances
        contact_info_elem = record.ContactInfo
        phone1 = format_phone_number(contact_info_elem.C_PHONE_1) if contact_info_elem is not None else None
        phone2 = format_phone_number(contact_info_elem.C_PHONE_2) if contact_info_elem is not None else None
        phone3 = format_phone_number(contact_info_elem.C_PHONE_3) if contact_info_elem is not None else None

        # BULLET POINT 5

        # Extract the national and local tax IDs from the TaxInfo NamedTuple
        tax_info_elem = record.TaxInfo
        national_tax_id = tax_info_elem.C_NAT_TX_ID if tax_info_elem is not None else None
        local_tax_id = tax_info_elem.C_LCL_TX_ID if tax_info_elem is not None else None

        # Perform the lookups
        national_tax_rate_desc, national_tax_rate = lookup_tax_rate(
            national_tax_id) if national_tax_id is not None else (None, None)
        local_tax_rate_desc, local_tax_rate = lookup_tax_rate(local_tax_id) if local_tax_id is not None else (
        None, None)

        # BULLET POINT 6
        # Check for 'UPDCUST' or 'INACT' status in the XML for the current customer ID
        has_updcust_or_inact = any(
            record.ActionType in ('UPDCUST', 'INACT')
            for record in records[i + 1:]
        )

        if not has_updcust_or_inact:
            # Proceed with extracting other information and performing the lookup
            # because there are no 'UPDCUST' or 'INACT' records for this Customer/@C_ID
            agency_id, credit_rating, net_worth, marketing_nameplate = lookup_prospect_data(last_name, first_name,
                                                                                            address_line1,
                                                                                            address_line2, postal_code)
        else:
            # If there are 'UPDCUST' or 'INACT' records for this customer,
            # these fields should be set to NULL as per the requirements
            agency_id = credit_rating = net_worth = marketing_nameplate = None

        #
        effective_date = datetime.datetime.now().strftime("%Y-%m-%d")  # nothing particular specified so set to now
        is_current = True
        end_date = datetime.datetime.strptime('9999-12-31', "%Y-%m-%d")

        # INACT
        if ActionType == "INACT":
            status = 'INACTIVE'
        else:
            status = 'ACTIVE'

        # Create a DimCustomer instance
        customer = RawDimCustomer(
            ActionType = ActionType,
            CustomerID=customer_id,
            TaxID=tax_id,
            LastName=last_name,
            FirstName=first_name,
            MiddleInitial=middle_initial,
            Tier=tier,
            DOB=dob,
            Email1=email1,
            Email2=email2,
            Gender=gender,
            AddressLine1=address_line1,
            AddressLine2=address_line2,
            PostalCode=postal_code,
            City=city,
            StateProv=state_prov,
            Country=country,
            Status=status,
            Phone1=phone1,
            Phone2=phone2,
            Phone3=phone3,
            NationalTaxRateDesc=national_tax_rate_desc,
            NationalTaxRate=national_tax_rate,
            LocalTaxRateDesc=local_tax_rate_desc,
            LocalTaxRate=local_tax_rate,
            AgencyID=agency_id,
            CreditRating=credit_rating,
            NetWorth=net_worth,
            MarketingNameplate=marketing_nameplate,
            IsCurrent=is_current,
            BatchID=1,
            EffectiveDate=effective_date,
            EndDate=end_date
        )
        customers.append(customer)

    return customers

def update_customers(customers: List[RawDimCustomer]) -> List[RawDimCustomer]:
    # Dictionary to hold the latest updates for each CustomerID, excluding the ActionType field
    latest_updates = {}

    # Process UPDCUST and INACT records to create update dictionaries
    for customer in customers:
        if customer.ActionType in ['UPDCUST', 'INACT']:
            # Store the latest non-None updates for each field except ActionType
            latest_updates[customer.CustomerID] = {
                f.name: getattr(customer, f.name)
                for f in fields(RawDimCustomer)
                if getattr(customer, f.name) is not None and f.name != 'ActionType'
            }

    # Apply the latest updates to NEW records
    for customer in customers:
        if customer.ActionType == 'NEW' and customer.CustomerID in latest_updates:
            # Apply the latest updates for this CustomerID, except for ActionType
            update_values = latest_updates[customer.CustomerID]
            for key, value in update_values.items():
                setattr(customer, key, value)

    # Filter out and return only the NEW entries
    return [customer for customer in customers if customer.ActionType == 'NEW']
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

def parse_customer_mgmt(xml_path: str) -> tuple[list[Customer], list[Account]]:
    namespace = {'TPCDI': 'http://www.tpc.org/tpc-di'}
    tree = ET.parse(xml_path)
    root = tree.getroot()

    customers: list[Customer] = []
    accounts: list[Account] = []

    for action_elem in root.findall('TPCDI:Action', namespace):
        action_type = action_elem.get('ActionType')
        action_ts = action_elem.get('ActionTS')

        customer_elem = action_elem.find('Customer', namespace)
        if customer_elem is not None:
            customer = Customer(
                C_ID=customer_elem.get('C_ID'),
                C_TAX_ID=customer_elem.get('C_TAX_ID'),
                C_GNDR=customer_elem.get('C_GNDR'),
                C_TIER=int(customer_elem.get('C_TIER')),
                C_DOB=customer_elem.get('C_DOB'),
                ActionType=action_type
            )

            temp = Name(
                C_L_NAME=customer_elem.findtext('./Name/C_L_NAME'),
                C_F_NAME=customer_elem.findtext('./Name/C_F_NAME'),
                C_M_NAME=customer_elem.find('./Name/C_M_NAME').text if customer_elem.find('./Name/C_M_NAME') is not None else None
            )
            customer.Name = temp

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

def run_etl(file_path: str):
    customers, accounts = parse_customer_mgmt(file_path)
    #dim_accounts = process_dim_account_records(accounts)
    dim_customers = process_dim_customer_records(customers)
    updated_dim_customers = update_customers(dim_customers)   # this does the UPDCAST etc

    print('h')


if __name__ == "__main__":
    file_path = 'CustomerMgmt.xml'  # Replace with your file path
    run_etl(file_path)