import csv
from enum import Enum
from typing import List, NamedTuple
from datetime import datetime

from date import DateRecord, parse_date_file

class RawProspect(NamedTuple):
    agency_id: str
    last_name: str
    first_name: str
    middle_initial: str
    gender: str
    address_line1: str
    address_line2: str
    postal_code: str
    city: str
    state: str
    country: str
    phone: str
    income: int
    number_cars: int
    number_children: int
    marital_status: str
    age: int
    credit_rating: int
    own_or_rent_flag: str
    employer: str
    number_credit_cards: int
    net_worth: int

# Define the ProcessedProspect NamedTuple for the processed data
class ProcessedProspect(NamedTuple):
    agency_id: str
    sk_record_date_id: int
    sk_update_date_id: int
    batch_id: int
    is_customer: bool
    last_name: str
    first_name: str
    middle_initial: str
    gender: str
    address_line1: str
    address_line2: str
    postal_code: str
    city: str
    state: str
    country: str
    phone: str
    income: int
    number_cars: int
    number_children: int
    marital_status: str
    age: int
    credit_rating: int
    own_or_rent_flag: str
    employer: str
    number_credit_cards: int
    net_worth: int
    marketing_nameplate: str

class MarketingTags(str, Enum):
    HIGH_VALUE = 'HighValue'
    EXPENSES = 'Expenses'
    BOOMER = 'Boomer'
    MONEY_ALERT = 'MoneyAlert'
    SPENDER = 'Spender'
    INHERITED = 'Inherited'

# Function to parse Prospect records from a CSV file
def parse_prospect_csv(csv_path: str) -> list[RawProspect]:
    raw_prospect_records = []
    
    with open(csv_path) as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            raw_prospect_record = RawProspect(
                agency_id=row['AgencyID'],
                last_name=row['LastName'],
                first_name=row['FirstName'],
                middle_initial=row['MiddleInitial'],
                gender=row['Gender'],
                address_line1=row['AddressLine1'],
                address_line2=row['AddressLine2'],
                postal_code=row['PostalCode'],
                city=row['City'],
                state=row['State'],
                country=row['Country'],
                phone=row['Phone'],
                income=int(row['Income']),
                number_cars=int(row['NumberCars']),
                number_children=int(row['NumberChildren']),
                marital_status=row['MaritalStatus'],
                age=int(row['Age']),
                credit_rating=int(row['CreditRating']),
                own_or_rent_flag=row['OwnOrRentFlag'],
                employer=row['Employer'],
                number_credit_cards=int(row['NumberCreditCards']),
                net_worth=int(row['NetWorth'])
            )
            raw_prospect_records.append(raw_prospect_record)
    
    return raw_prospect_records

# Function to process RawProspect records and return ProcessedProspect records
def process_raw_prospect_records(records: List[RawProspect], batch_date: datetime, dates: list[DateRecord]) -> List[ProcessedProspect]:
    processed_prospect_records = []

    for raw_prospect in records:
        # Set SK_RecordDateID and SK_UpdateDateID
        sk_record_date_id = get_dim_date_sk_id(batch_date, dates)
        sk_update_date_id = sk_record_date_id  # Assume SK_UpdateDateID is the same as SK_RecordDateID

        # Check if the prospect matches a current customer record in DimCustomer
        is_customer = check_matching_customer(raw_prospect)

        # Set MarketingNameplate based on defined tags
        marketing_nameplate = set_marketing_nameplate(raw_prospect)

        # Set BatchID
        batch_id = set_batch_id()

        # Create a ProcessedProspect object
        processed_prospect = ProcessedProspect(
            agency_id=raw_prospect.agency_id,
            sk_record_date_id=sk_record_date_id,
            sk_update_date_id=sk_update_date_id,
            batch_id=batch_id,
            is_customer=is_customer,
            last_name=raw_prospect.last_name,
            first_name=raw_prospect.first_name,
            middle_initial=raw_prospect.middle_initial,
            gender=raw_prospect.gender,
            address_line1=raw_prospect.address_line1,
            address_line2=raw_prospect.address_line2,
            postal_code=raw_prospect.postal_code,
            city=raw_prospect.city,
            state=raw_prospect.state,
            country=raw_prospect.country,
            phone=raw_prospect.phone,
            income=raw_prospect.income,
            number_cars=raw_prospect.number_cars,
            number_children=raw_prospect.number_children,
            marital_status=raw_prospect.marital_status,
            age=raw_prospect.age,
            credit_rating=raw_prospect.credit_rating,
            own_or_rent_flag=raw_prospect.own_or_rent_flag,
            employer=raw_prospect.employer,
            number_credit_cards=raw_prospect.number_credit_cards,
            net_worth=raw_prospect.net_worth,
            marketing_nameplate=marketing_nameplate
        )

        # Append to the list of processed prospect records
        processed_prospect_records.append(processed_prospect)

    # After processing all records, write a status message to DImessages table
    status_message = f"Status: Inserted rows - {len(processed_prospect_records)}"
    print(status_message)
    # Example query (pseudo-code):
    # query = "INSERT INTO DImessages (MessageSource, MessageText, MessageData) VALUES (?, ?, ?)"
    # execute_query(query, ["Prospect", "Inserted rows", str(len(processed_prospect_records))])

    return processed_prospect_records

# Replace the following example functions with actual implementation
def get_dim_date_sk_id(date: datetime, dates: list[DimDate]) -> int:
    # Replace with actual logic to get SK_DateID from DimDate based on the given date
    return next(d.dateID for d in dates if d.DateValue == date)

def check_matching_customer(prospect: RawProspect, dim_customers: list[DimCustomer]) -> bool:
    """IsCustomer is set to True or False depending on whether the prospective customer record
    matches a current customer record in DimCustomer whose status is ‘ACTIVE’ after all
    customer records in the batch have been processed. A Prospect record is deemed to
    match a DimCustomer record if the FirstName, LastName, AddressLine1, AddressLine2
    and PostalCode fields all match when upper-cased."""
    # Replace with logic to check if the prospect matches a current customer in DimCustomer
    return bool(next((customer for customer in dim_customers if tuple(map(str.upper, (
        prospect.first_name, prospect.last_name, prospect.address_line1,
        prospect.address_line2, prospect.postal_code))) == ((customer.first_name, customer.last_name,
        customer.address_line1, customer.address_line2, customer.postal_code))), False))

def set_marketing_nameplate(prospect: RawProspect) -> str:
    # Define the order of tags
    tag_order = [
        MarketingTags.HIGH_VALUE,
        MarketingTags.EXPENSES,
        MarketingTags.BOOMER,
        MarketingTags.MONEY_ALERT,
        MarketingTags.SPENDER,
        MarketingTags.INHERITED
    ]

    # Initialize an empty list to store matching tags
    matching_tags = []

    # Check each tag condition and add to the list if the condition is met
    for tag in tag_order:
        if tag == MarketingTags.HIGH_VALUE and (prospect.net_worth > 1000000 or prospect.income > 200000):
            matching_tags.append(tag.value)
        elif tag == MarketingTags.EXPENSES and (prospect.number_children > 3 or prospect.number_credit_cards > 5):
            matching_tags.append(tag.value)
        elif tag == MarketingTags.BOOMER and prospect.age > 45:
            matching_tags.append(tag.value)
        elif tag == MarketingTags.MONEY_ALERT and (prospect.income < 50000 or prospect.credit_rating < 600 or prospect.net_worth < 100000):
            matching_tags.append(tag.value)
        elif tag == MarketingTags.SPENDER and (prospect.number_cars > 3 or prospect.number_credit_cards > 7):
            matching_tags.append(tag.value)
        elif tag == MarketingTags.INHERITED and (prospect.age < 25 and prospect.net_worth > 1000000):
            matching_tags.append(tag.value)

    # Concatenate the matching tags with '+'
    marketing_nameplate = '+'.join(matching_tags)

    return marketing_nameplate if matching_tags else None  # Return None if no tags apply

def set_batch_id() -> int:
    # Replace with logic to set BatchID
    return 1  # Example value, replace with actual logic

# Example usage:
# Replace 'prospect.csv' with the actual CSV file path
csv_file_path = 'prospect.csv'
prospect_records = parse_prospect_csv(csv_file_path)
batch_date = datetime.now()  # Replace with the actual batch date

dates = parse_date_file("data/data/Date.txt")

# Process Prospect records
parse_prospect_csv(prospect_records, batch_date, dates)
