import dataclasses
import os
from typing import NamedTuple
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from typing import List, Optional
from sqlalchemy import text
from collections import namedtuple
from typing import List
import datetime
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

DimCustomer = namedtuple('DimCustomer', [
    'SK_CustomerID', 'CustomerID', 'TaxID', 'Status', 'LastName', 'FirstName',
    'MiddleInitial', 'Gender', 'Tier', 'DOB', 'AddressLine1', 'AddressLine2',
    'PostalCode', 'City', 'StateProv', 'Country', 'Phone1', 'Phone2', 'Phone3',
    'Email1', 'Email2', 'NationalTaxRateDesc', 'NationalTaxRate', 'LocalTaxRateDesc',
    'LocalTaxRate', 'AgencyID', 'CreditRating', 'NetWorth', 'MarketingNameplate',
    'IsCurrent', 'BatchID', 'EffectiveDate', 'EndDate'
])


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

class DimCustomer(NamedTuple):
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


def parse(file_path: str) -> List[RawDimCustomer]:
    namespace = {'TPCDI': 'http://www.tpc.org/tpc-di'}
    tree = ET.parse(file_path)
    root = tree.getroot()

    customers = []
    count = 0
    for action in root.findall('TPCDI:Action', namespace):
        #temp just for testing
        count+=1
        if count > 100:
            break
        customer_elem = action.find('Customer', namespace)

        ActionType = action.get('ActionType')
        # BULLET POINT 1
        # Extracting attributes directly from Customer element
        customer_id = customer_elem.get('C_ID')
        tax_id = customer_elem.get('C_TAX_ID')
        # Extracting and safely converting Tier
        tier = customer_elem.get('C_TIER')
        try:
            tier = int(tier) if tier is not None else None
        except ValueError:
            tier = None
        dob = customer_elem.get('C_DOB')

        # Extracting values from the Name sub-element
        name_elem = customer_elem.find('Name')
        last_name = name_elem.find('C_L_NAME').text if name_elem is not None and name_elem.find(
            'C_L_NAME') is not None else None
        first_name = name_elem.find('C_F_NAME').text if name_elem is not None and name_elem.find(
            'C_F_NAME') is not None else None
        middle_initial = name_elem.find('C_M_NAME').text if name_elem is not None and name_elem.find(
            'C_M_NAME') is not None else None

        # Extracting values from the ContactInfo sub-element
        contact_info_elem = customer_elem.find('ContactInfo')
        email1 = contact_info_elem.find(
            'C_PRIM_EMAIL').text if contact_info_elem is not None and contact_info_elem.find(
            'C_PRIM_EMAIL') is not None else None
        email2 = contact_info_elem.find(
            'C_ALT_EMAIL').text if contact_info_elem is not None and contact_info_elem.find(
            'C_ALT_EMAIL') is not None else None

        # BULLET POINT 2
        # Extracting and processing the Gender attribute
        gender = customer_elem.get('C_GNDR', 'U').upper()  # Default to 'U' if not present
        if gender not in ('M', 'F'):
            gender = 'U'

        # BULLET POINT 3

        # Extracting values from the Address sub-element
        address_elem = customer_elem.find('Address')
        address_line1 = address_elem.find('C_ADLINE1').text if address_elem is not None and address_elem.find(
            'C_ADLINE1') is not None else None
        address_line2 = address_elem.find('C_ADLINE2').text if address_elem is not None and address_elem.find(
            'C_ADLINE2') is not None else None
        postal_code = address_elem.find('C_ZIPCODE').text if address_elem is not None and address_elem.find(
            'C_ZIPCODE') is not None else None
        city = address_elem.find('C_CITY').text if address_elem is not None and address_elem.find(
            'C_CITY') is not None else None
        state_prov = address_elem.find('C_STATE_PROV').text if address_elem is not None and address_elem.find(
            'C_STATE_PROV') is not None else None
        country = address_elem.find('C_CTRY').text if address_elem is not None and address_elem.find(
            'C_CTRY') is not None else None

        # BULLET POINT 4
        # Function to format phone numbers
        def format_phone_number(phone_element):
            if phone_element is None:
                return None

            country_code = phone_element.find('C_CTRY_CODE').text if phone_element.find(
                'C_CTRY_CODE') is not None else None
            area_code = phone_element.find('C_AREA_CODE').text if phone_element.find(
                'C_AREA_CODE') is not None else None
            local = phone_element.find('C_LOCAL').text if phone_element.find('C_LOCAL') is not None else None
            ext = phone_element.find('C_EXT').text if phone_element.find('C_EXT') is not None else None

            # Apply formatting based on the presence of components
            if country_code and area_code and local:
                phone = f"+{country_code} ({area_code}) {local}"
            elif not country_code and area_code and local:
                phone = f"({area_code}) {local}"
            elif not area_code and local:
                phone = local
            else:
                return None

            # Append extension if present
            if ext:
                phone += f" {ext}"

            return phone

        contact_info_elem = customer_elem.find('ContactInfo')
        phone1 = format_phone_number(contact_info_elem.find('C_PHONE_1')) if contact_info_elem is not None else None
        phone2 = format_phone_number(contact_info_elem.find('C_PHONE_2')) if contact_info_elem is not None else None
        phone3 = format_phone_number(contact_info_elem.find('C_PHONE_3')) if contact_info_elem is not None else None

        # BULLET POINT 5

        # Extract the national and local tax IDs from the XML
        national_tax_id = customer_elem.find('TaxInfo/C_NAT_TX_ID').text if customer_elem is not None and customer_elem.find('TaxInfo/C_NAT_TX_ID') is not None else None
        local_tax_id = customer_elem.find('TaxInfo/C_LCL_TX_ID').text if customer_elem is not None and customer_elem.find('TaxInfo/C_LCL_TX_ID') is not None else None

        # Perform the lookups
        national_tax_rate_desc, national_tax_rate = lookup_tax_rate(national_tax_id)
        local_tax_rate_desc, local_tax_rate = lookup_tax_rate(local_tax_id)

        # BULLET POINT 6
        # Check for 'UPDCUST' or 'INACT' status in the XML for the current customer ID
        update_status_elems = root.findall(f".//TPCDI:Customer[@C_ID='{customer_id}']/TPCDI:Status", namespace)
        has_updcust_or_inact = any(elem.text in ('UPDCUST', 'INACT') for elem in update_status_elems)

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


def extract(file_path: str) -> List[DimCustomer]:
    namespace = {'TPCDI': 'http://www.tpc.org/tpc-di'}
    tree = ET.parse(file_path)
    root = tree.getroot()

    customers = []
    for action in root.findall('TPCDI:Action', namespace):
        customer_elem = action.find('Customer', namespace)

        # BULLET POINT 1
        # Extracting attributes directly from Customer element
        customer_id = customer_elem.get('C_ID')
        tax_id = customer_elem.get('C_TAX_ID')
        # Extracting and safely converting Tier
        tier = customer_elem.get('C_TIER')
        try:
            Tier = int(tier) if tier is not None else None
        except ValueError:
            Tier = None
        dob = customer_elem.get('C_DOB')

        # Extracting values from the Name sub-element
        name_elem = customer_elem.find('Name')
        last_name = name_elem.find('C_L_NAME').text if name_elem is not None and name_elem.find(
            'C_L_NAME') is not None else None
        first_name = name_elem.find('C_F_NAME').text if name_elem is not None and name_elem.find(
            'C_F_NAME') is not None else None
        middle_initial = name_elem.find('C_M_NAME').text if name_elem is not None and name_elem.find(
            'C_M_NAME') is not None else None

        # Extracting values from the ContactInfo sub-element
        contact_info_elem = customer_elem.find('ContactInfo')
        email1 = contact_info_elem.find(
            'C_PRIM_EMAIL').text if contact_info_elem is not None and contact_info_elem.find(
            'C_PRIM_EMAIL') is not None else None
        email2 = contact_info_elem.find(
            'C_ALT_EMAIL').text if contact_info_elem is not None and contact_info_elem.find(
            'C_ALT_EMAIL') is not None else None

        # BULLET POINT 2
        # Extracting and processing the Gender attribute
        gender = customer_elem.get('C_GNDR', 'U').upper()  # Default to 'U' if not present
        if gender not in ('M', 'F'):
            gender = 'U'

        # BULLET POINT 3

        # Extracting values from the Address sub-element
        address_elem = customer_elem.find('Address')
        address_line1 = address_elem.find('C_ADLINE1').text if address_elem is not None and address_elem.find(
            'C_ADLINE1') is not None else None
        address_line2 = address_elem.find('C_ADLINE2').text if address_elem is not None and address_elem.find(
            'C_ADLINE2') is not None else None
        postal_code = address_elem.find('C_ZIPCODE').text if address_elem is not None and address_elem.find(
            'C_ZIPCODE') is not None else None
        city = address_elem.find('C_CITY').text if address_elem is not None and address_elem.find(
            'C_CITY') is not None else None
        state_prov = address_elem.find('C_STATE_PROV').text if address_elem is not None and address_elem.find(
            'C_STATE_PROV') is not None else None
        country = address_elem.find('C_CTRY').text if address_elem is not None and address_elem.find(
            'C_CTRY') is not None else None

        # BULLET POINT 4
        # Function to format phone numbers
        def format_phone_number(phone_element):
            if phone_element is None:
                return None

            country_code = phone_element.find('C_CTRY_CODE').text if phone_element.find(
                'C_CTRY_CODE') is not None else None
            area_code = phone_element.find('C_AREA_CODE').text if phone_element.find(
                'C_AREA_CODE') is not None else None
            local = phone_element.find('C_LOCAL').text if phone_element.find('C_LOCAL') is not None else None
            ext = phone_element.find('C_EXT').text if phone_element.find('C_EXT') is not None else None

            # Apply formatting based on the presence of components
            if country_code and area_code and local:
                phone = f"+{country_code} ({area_code}) {local}"
            elif not country_code and area_code and local:
                phone = f"({area_code}) {local}"
            elif not area_code and local:
                phone = local
            else:
                return None

            # Append extension if present
            if ext:
                phone += f" {ext}"

            return phone

        contact_info_elem = customer_elem.find('ContactInfo')
        phone1 = format_phone_number(contact_info_elem.find('C_PHONE_1')) if contact_info_elem is not None else None
        phone2 = format_phone_number(contact_info_elem.find('C_PHONE_2')) if contact_info_elem is not None else None
        phone3 = format_phone_number(contact_info_elem.find('C_PHONE_3')) if contact_info_elem is not None else None

        # BULLET POINT 5
        # Extract the national and local tax IDs from the XML
        national_tax_id = customer_elem.find('TaxInfo/C_NAT_TX_ID').text
        local_tax_id = customer_elem.find('TaxInfo/C_LCL_TX_ID').text

        # Perform the lookups
        national_tax_rate_desc, national_tax_rate = lookup_tax_rate(national_tax_id)
        local_tax_rate_desc, local_tax_rate = lookup_tax_rate(local_tax_id)

        # BULLET POINT 6
        # Check for 'UPDCUST' or 'INACT' status in the XML for the current customer ID
        update_status_elems = root.findall(f".//TPCDI:Customer[@C_ID='{customer_id}']/TPCDI:Status", namespace)
        has_updcust_or_inact = any(elem.text in ('UPDCUST', 'INACT') for elem in update_status_elems)

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

        # Create a DimCustomer instance
        customer = DimCustomer(
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
            Status='ACTIVE',  # Set Status to 'ACTIVE'
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


def create_dim_customer_table(engine):
    # SQL statement for creating the DimCustomer table
    create_table_stmt = text("""
    CREATE TABLE IF NOT EXISTS public.DimCustomer (
        SK_CustomerID SERIAL PRIMARY KEY, --database manages this
        CustomerID CHAR(10) NOT NULL,
        TaxID CHAR(20) NOT NULL,
        Status CHAR(10) NOT NULL,
        LastName CHAR(30) NOT NULL,
        FirstName CHAR(30) NOT NULL,
        MiddleInitial CHAR(1),
        Gender CHAR(1),
        Tier NUMERIC(1),
        DOB DATE NOT NULL,
        AddressLine1 CHAR(80) NOT NULL,
        AddressLine2 CHAR(80),
        PostalCode CHAR(12) NOT NULL,
        City CHAR(25) NOT NULL,
        StateProv CHAR(20) NOT NULL,
        Country CHAR(24) NOT NULL,
        Phone1 CHAR(30),
        Phone2 CHAR(30),
        Phone3 CHAR(30),
        Email1 CHAR(50),
        Email2 CHAR(50),
        NationalTaxRateDesc CHAR(50),
        NationalTaxRate NUMERIC(6,5),
        LocalTaxRateDesc CHAR(50),
        LocalTaxRate NUMERIC(6,5),
        AgencyID CHAR(30),
        CreditRating NUMERIC(5),
        NetWorth NUMERIC(10),
        MarketingNameplate CHAR(100),
        IsCurrent BOOLEAN NOT NULL,
        BatchID NUMERIC(5) NOT NULL,
        EffectiveDate DATE NOT NULL,
        EndDate DATE NOT NULL
    );
    """)

    # Execute the create table statement
    with engine.connect() as connection:
        connection.execute(create_table_stmt)


def load(rows: list[DimCustomer]):
    # Define SQL statements for table creation and data insertion
    create_dim_customer_table(engine)

    # Convert the named tuple to a list of dictionaries
    rows_as_dicts = [row._asdict() for row in rows]

    # Define the SQL insert statement including the new fields
    insert_stmt = text("""
                INSERT INTO DimCustomer (CustomerID, TaxID, Status, LastName, FirstName, 
                MiddleInitial, Gender, Tier, DOB, AddressLine1, AddressLine2, PostalCode, City, StateProv, 
                Country, Phone1, Phone2, Phone3, Email1, Email2, NationalTaxRateDesc, NationalTaxRate, 
                LocalTaxRateDesc, LocalTaxRate, AgencyID, CreditRating, NetWorth, MarketingNameplate,
                IsCurrent, BatchID, EffectiveDate, EndDate) 
                VALUES (:CustomerID, :TaxID, :Status, :LastName, :FirstName, 
                :MiddleInitial, :Gender, :Tier, :DOB, :AddressLine1, :AddressLine2, :PostalCode, :City, 
                :StateProv, :Country, :Phone1, :Phone2, :Phone3, :Email1, :Email2, :NationalTaxRateDesc, 
                :NationalTaxRate, :LocalTaxRateDesc, :LocalTaxRate, :AgencyID, :CreditRating, :NetWorth, 
                :MarketingNameplate, :IsCurrent, :BatchID, :EffectiveDate, :EndDate)
            """)

    # Execute the bulk insert using a transaction for safety
    with engine.begin() as conn:
        conn.execute(insert_stmt, rows_as_dicts)

def convert_new_raw_to_dim(customers: List[RawDimCustomer]) -> List[DimCustomer]:
    new_customers = []

    for customer in customers:
        if customer.ActionType == 'NEW':
            # Convert the RawDimCustomer instance to a DimCustomer NamedTuple
            dim_customer = DimCustomer(
                CustomerID=customer.CustomerID,
                TaxID=customer.TaxID,
                Status=customer.Status,
                LastName=customer.LastName,
                FirstName=customer.FirstName,
                MiddleInitial=customer.MiddleInitial,
                Gender=customer.Gender,
                Tier=customer.Tier,
                DOB=customer.DOB,
                AddressLine1=customer.AddressLine1,
                AddressLine2=customer.AddressLine2,
                PostalCode=customer.PostalCode,
                City=customer.City,
                StateProv=customer.StateProv,
                Country=customer.Country,
                Phone1=customer.Phone1,
                Phone2=customer.Phone2,
                Phone3=customer.Phone3,
                Email1=customer.Email1,
                Email2=customer.Email2,
                NationalTaxRateDesc=customer.NationalTaxRateDesc,
                NationalTaxRate=customer.NationalTaxRate,
                LocalTaxRateDesc=customer.LocalTaxRateDesc,
                LocalTaxRate=customer.LocalTaxRate,
                AgencyID=customer.AgencyID,
                CreditRating=customer.CreditRating,
                NetWorth=customer.NetWorth,
                MarketingNameplate=customer.MarketingNameplate,
                IsCurrent=customer.IsCurrent,
                BatchID=customer.BatchID,
                EffectiveDate=customer.EffectiveDate,
                EndDate=customer.EndDate
            )
            new_customers.append(dim_customer)

    return new_customers
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

def run_etl(file_path: str):
    parse_list = parse(file_path)
    temp=update_customers(parse_list)
    final = convert_new_raw_to_dim(temp)
    load(final)

if __name__ == "__main__":
    file_path = 'CustomerMgmt.xml'  # Replace with your file path
    run_etl(file_path)