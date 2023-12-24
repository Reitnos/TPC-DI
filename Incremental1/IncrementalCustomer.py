import json
import os
import json
import boto3
import botocore
import redshift_connector
import botocore.session as bc
from botocore.client import Config
from typing import NamedTuple
from datetime import date

from itertools import islice

# print('Loading function')
#DONE
session = boto3.session.Session()
region_name = session.region_name

# Initializing Botocore client
bc_session = bc.get_session()

session = boto3.Session(
        botocore_session=bc_session,
        region_name=region_name
    )
s3_client = session.client("s3")
client_redshift = session.client("redshift-data")
secret_name = "redshift!tpcdi-benchmark-admin"

import psycopg2
from psycopg2 import sql

# Replace these with your actual database connection details
# dbname = 'tpcdi'
# user = 'reitnos'
# password = '2690'
# host = 'localhost'
# port = '5432'

# # Establish a connection to the PostgreSQL database
# conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# #print the databases
# cur = conn.cursor()

class Customer(NamedTuple):
    CDC_FLAG: str  # 'I' for insert or 'U' for update
    SK_CustomerID: int  # Surrogate key for CustomerID
    CustomerID: str  # Customer identifier
    TaxID: str  # Customer’s tax identifier
    Status: str  # Customer status type
    LastName: str  # Customer's last name
    FirstName: str  # Customer's first name
    MiddleInitial: str  # Customer's middle name initial
    Gender: str  # Gender of the customer
    Tier: int  # Customer tier
    DOB: date  # Customer’s date of birth
    AddressLine1: str  # Address Line 1
    AddressLine2: str  # Address Line 2
    PostalCode: str  # Zip or Postal Code
    City: str  # City
    StateProv: str  # State or Province
    Country: str  # Country
    Phone1: str  # Phone number 1
    Phone2: str  # Phone number 2
    Phone3: str  # Phone number 3
    Email1: str  # Email address 1
    Email2: str  # Email address 2
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
    EffectiveDate: date  # Beginning of date range when this record was the current record
    EndDate: date  # Ending of date range when this record was the current record

class ProspectIncremental(NamedTuple):
    AgencyID: str  # Unique identifier from agency
    LastName: str  # Last name
    FirstName: str  # First name
    MiddleInitial: str  # Middle initial
    Gender: str  # 'M' or 'F' or 'U'
    AddressLine1: str  # Postal address
    AddressLine2: str  # Postal address
    PostalCode: str  # Postal code
    City: str  # City
    State: str  # State or province
    Country: str  # Postal country
    Phone: str  # Telephone number
    Income: int  # Annual income
    NumberCars: int  # Cars owned
    NumberChildren: int  # Dependent children
    MaritalStatus: str  # 'S' or 'M' or 'D' or 'W' or 'U'
    Age: int  # Current age
    CreditRating: int  # Numeric rating
    OwnOrRentFlag: str  # 'O' or 'R' or 'U'
    Employer: str  # Name of employer
    NumberCreditCards: int  # Credit cards
    NetWorth: int  # Estimated total net worth

class CustomerIncremental(NamedTuple):
    CDC_FLAG: str  # 'I' for insert or 'U' for update
    CDC_DSN: int  # Database Sequence Number, Not NULL
    C_ID: str  # Customer identifier, Not NULL
    C_TAX_ID: str  # Customer’s tax identifier, Not NULL
    C_ST_ID: str  # 'ACTV' or 'INAC', Customer status type identifier
    C_L_NAME: str  # Primary Customer's last name, Not NULL
    C_F_NAME: str  # Primary Customer's first name, Not NULL
    C_M_NAME: str  # Primary Customer's middle initial
    C_GNDR: str  # Gender of the primary customer
    C_TIER: int  # Customer tier
    C_DOB: date  # Customer’s date of birth, as YYYY-MM-DD, Not NULL
    C_ADLINE1: str  # Address Line 1, Not NULL
    C_ADLINE2: str  # Address Line 2
    C_ZIPCODE: str  # Zip or postal code, Not NULL
    C_CITY: str  # City, Not NULL
    C_STATE_PROV: str  # State or province, Not NULL
    C_CTRY: str  # Country
    C_CTRY_1: str  # Country code for Customer's phone 1.
    C_AREA_1: str  # Area code for customer’s phone 1.
    C_LOCAL_1: str  # Local number for customer’s phone 1.
    C_EXT_1: str  # Extension number for Customer’s phone 1.
    C_CTRY_2: str  # Country code for Customer's phone 2.
    C_AREA_2: str  # Area code for Customer’s phone 2.
    C_LOCAL_2: str  # Local number for Customer’s phone 2.
    C_EXT_2: str  # Extension number for Customer’s phone 2.
    C_CTRY_3: str  # Country code for Customer's phone 3.
    C_AREA_3: str  # Area code for Customer’s phone 3.
    C_LOCAL_3: str  # Local number for Customer’s phone 3.
    C_EXT_3: str  # Extension number for Customer’s phone 3.
    C_EMAIL_1: str  # Customer's e-mail address 1.
    C_EMAIL_2: str  # Customer's e-mail address 2.
    C_LCL_TX_ID: str  # Customer's local tax rate, Not NULL
    C_NAT_TX_ID: str  # Customer's national tax rate, Not NULL

def parse_prospect_incremental(line: str) -> ProspectIncremental:
    values = line.strip().split(',')

    return ProspectIncremental(
        AgencyID=values[0],
        LastName=values[1],
        FirstName=values[2],
        MiddleInitial=values[3],
        Gender=values[4],
        AddressLine1=values[5],
        AddressLine2=values[6],
        PostalCode=values[7],
        City=values[8],
        State=values[9],
        Country=values[10],
        Phone=values[11],
        Income=int(values[12]) if values[12] else None,
        NumberCars=int(values[13]) if values[13] else None,
        NumberChildren=int(values[14]) if values[14] else None,
        MaritalStatus=values[15],
        Age=int(values[16]) if values[16] else None,
        CreditRating=int(values[17]) if values[17] else None,
        OwnOrRentFlag=values[18],
        Employer=values[19],
        NumberCreditCards=int(values[20]) if values[20] else None,
        NetWorth=int(values[21]) if values[21] else None
    )


def parse_customer_incremental(line: str) -> CustomerIncremental:
    values = line.strip().split('|')

    return CustomerIncremental(
        CDC_FLAG=values[0],
        CDC_DSN=int(values[1]),
        C_ID=values[2],
        C_TAX_ID=values[3],
        C_ST_ID=values[4],
        C_L_NAME=values[5],
        C_F_NAME=values[6],
        C_M_NAME=values[7],
        C_GNDR=values[8],
        C_TIER=int(values[9]) if values[9] else None,
        C_DOB=date.fromisoformat(values[10]),
        C_ADLINE1=values[11],
        C_ADLINE2=values[12],
        C_ZIPCODE=values[13],
        C_CITY=values[14],
        C_STATE_PROV=values[15],
        C_CTRY=values[16],
        C_CTRY_1=values[17],
        C_AREA_1=values[18],
        C_LOCAL_1=values[19],
        C_EXT_1=values[20],
        C_CTRY_2=values[21],
        C_AREA_2=values[22],
        C_LOCAL_2=values[23],
        C_EXT_2=values[24],
        C_CTRY_3=values[25],
        C_AREA_3=values[26],
        C_LOCAL_3=values[27],
        C_EXT_3=values[28],
        C_EMAIL_1=values[29],
        C_EMAIL_2=values[30],
        C_LCL_TX_ID=values[31],
        C_NAT_TX_ID=values[32]
    )

def extract() -> list[CustomerIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/Customer.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/Customer2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    customerInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        customerInstance = parse_customer_incremental(stripped_line)
        customerInstanceList.append(customerInstance)

    return customerInstanceList
def transform(raw_rows):

    customers = []

    for row in raw_rows:
        
        gender = row.C_GNDR
        if row.C_GNDR != 'M' or row.C_GNDR != 'F' or row.C_GNDR == '':
            gender = 'U'
        #search StatusType table in postgresql and find the ST_NAME for the ST_ID = row.C_ST_ID

        sql_for_st_name = f"SELECT ST_NAME FROM public.StatusType WHERE ST_ID = {row.C_ST_ID};"
        #sql_for_st_name = sql.SQL("SELECT ST_NAME FROM public.StatusType WHERE ST_ID = %s;")
        
        #cur.execute(sql_for_st_name, (row.C_ST_ID,))

        # Fetch all the rows
        #queryresults = cur.fetchone()

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_st_name)
        queryresults = client_redshift.fetchone()
        st_name = queryresults[0]
   
        #st_name =  postgresql.execute(sql_for_st_name)
        Phone1 = ""
        Phone2 = ""
        Phone3 = ""
        for i in range (1,4):
            
            

            if i == 1:
                if row.C_CTRY_1 != '' and row.C_AREA_1 != '' and row.C_LOCAL_1 != '':
                    Phone1 = '+' + row.C_CTRY_1 + '(' + row.C_AREA_1 + ')' + row.C_LOCAL_1 
                    Phone1 = Phone1 + row.C_EXT_1
                elif row.C_CTRY_1 == '' and row.C_AREA_1 != '' and row.C_LOCAL_1 != '':
                    Phone1 = '(' + row.C_AREA_1 + ')' + row.C_LOCAL_1
                    Phone1 = Phone1 + row.C_EXT_1
                elif row.C_AREA_1 == '' and row.C_LOCAL_1 != '':
                    Phone1 = row.C_LOCAL_1
                    Phone1 = Phone1 + row.C_EXT_1
                else:
                    Phone1 = ''
            if i == 2:
                if row.C_CTRY_2 != '' and row.C_AREA_2 != '' and row.C_LOCAL_2 != '':
                    Phone2 = '+' + row.C_CTRY_2 + '(' + row.C_AREA_2 + ')' + row.C_LOCAL_2
                    Phone2 = Phone2 + row.C_EXT_2
                elif row.C_CTRY_2 == '' and row.C_AREA_2 != '' and row.C_LOCAL_2 != '':
                    Phone2 = '(' + row.C_AREA_2 + ')' + row.C_LOCAL_2
                    Phone2 = Phone2 + row.C_EXT_2
                elif row.C_AREA_2 == '' and row.C_LOCAL_2 != '':
                    Phone2 = row.C_LOCAL_2
                    Phone2 = Phone2 + row.C_EXT_2
                else:
                    Phone2 = ''
                
            
            if i == 3:
                if row.C_CTRY_3 != '' and row.C_AREA_3 != '' and row.C_LOCAL_3 != '':
                    Phone3 = '+' + row.C_CTRY_3 + '(' + row.C_AREA_3 + ')' + row.C_LOCAL_3
                    Phone3 = Phone3 + row.C_EXT_3
                elif row.C_CTRY_3 == '' and row.C_AREA_3 != '' and row.C_LOCAL_3 != '':
                    Phone3 = '(' + row.C_AREA_3 + ')' + row.C_LOCAL_3
                    Phone3 = Phone3 + row.C_EXT_3
                elif row.C_AREA_3 == '' and row.C_LOCAL_3 != '':
                    Phone3 = row.C_LOCAL_3
                    Phone3 = Phone3 + row.C_EXT_3
                else:
                    Phone3 = ''
    
        # print(Phone1)
        # print(Phone2)
        # print(Phone3)
        #AgencyID, CreditRating, NetWorth, MarketingNameplate: should populate from prospectInstance.csv 
        #check if prospectInstance.csv has any matching DimCustomer record if the FirstName, LastName, AddressLine1, AddressLine2 and PostalCode fields all match the corresponding fields in DimCustomer when uppercased.
        #if there is a match, then update the record in DimCustomer with the values from prospectInstance.csv

        #read from prospectInstance.csv file
        credit_rating = 0
        net_worth = 0
        agency_id = ""
        marketing_nameplate = ""
        prospect_csv_string = ""

        response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/Prospet.csv',
        )
        prospect_csv_string = response['Body'].read().decode('utf-8')
        # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/Prospect.csv', 'r') as f:
        #     prospect_csv_string = f.read()
        
        lines = prospect_csv_string.splitlines()
        for line in lines:
            stripped_line = line.strip()
            prospectInstance = parse_prospect_incremental(stripped_line)
            if prospectInstance.FirstName.upper() == row.C_F_NAME.upper() and prospectInstance.LastName.upper() == row.C_L_NAME.upper() and prospectInstance.AddressLine1.upper() == row.C_ADLINE1.upper() and prospectInstance.AddressLine2.upper() == row.C_ADLINE2.upper() and prospectInstance.PostalCode.upper() == row.C_ZIPCODE.upper():
                credit_rating = prospectInstance.CreditRating
                net_worth = prospectInstance.NetWorth
                agency_id = prospectInstance.AgencyID

                tags = []

                # Check conditions for each tag and append to the list
                
                if prospectInstance.NetWorth != None and prospectInstance.Income != None and (prospectInstance.NetWorth > 1000000 or prospectInstance.Income > 200000):
                    tags.append("HighValue")

                if prospectInstance.NumberChildren != None and prospectInstance.NumberCreditCards != None and (prospectInstance.NumberChildren > 3 or prospectInstance.NumberCreditCards > 5):
                    tags.append("Expenses")

                if prospectInstance.Age != None and  prospectInstance.Age > 45:
                    tags.append("Boomer")

                if prospectInstance.Income != None and prospectInstance.CreditRating != None and  prospectInstance.NetWorth != None and (prospectInstance.Income < 50000 or prospectInstance.CreditRating < 600 or prospectInstance.NetWorth < 100000):
                    tags.append("MoneyAlert")

                if prospectInstance.NumberCars != None and prospectInstance.NumberCreditCards != None and (prospectInstance.NumberCars > 3 or prospectInstance.NumberCreditCards > 7):
                    tags.append("Spender")

                if prospectInstance.Age != None and prospectInstance.NetWorth != None and (prospectInstance.Age < 25 and prospectInstance.NetWorth > 1000000):
                    tags.append("Inherited")

                # Concatenate tags with '+' or return None if no tags apply
                marketing_nameplate =  '+'.join(tags) if tags else None
                # print("MATCHED: ")
                # print(marketing_nameplate)  
                # print(prospectInstance)
        lcl_tx_name = ""
        lcl_tx_rate = None
        nat_tx_name = ""
        nat_tx_rate = None
        #search TaxRate table in postgresql and find the TX_DESCRIPTION for the TX_ID = row.C_LCL_TX_ID


        sql_for_tx_description_and_rate = f"SELECT TX_NAME,TX_RATE FROM public.TaxRate WHERE TX_ID = {row.C_LCL_TX_ID};"
        #sql_for_tx_description_and_rate = sql.SQL("SELECT TX_NAME,TX_RATE FROM public.TaxRate WHERE TX_ID = %s;")
        #cur.execute(sql_for_tx_description_and_rate, (row.C_LCL_TX_ID,))

        # Fetch all the rows
        #queryresults = cur.fetchone()

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_tx_description_and_rate)
        queryresults = client_redshift.fetchone()

        if queryresults != None:
            lcl_tx_name = queryresults[0]
            lcl_tx_rate = queryresults[1]
        # print(tx_name)
        # print(tx_rate)
            
        sql_for_tx_description_and_rate = f"SELECT TX_NAME,TX_RATE FROM public.TaxRate WHERE TX_ID = {row.C_NAT_TX_ID};"
        
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_tx_description_and_rate)
        queryresults = client_redshift.fetchone()

        #cur.execute(sql_for_tx_description_and_rate, (row.C_NAT_TX_ID,))
        #queryresults = cur.fetchone()
        if queryresults != None:
            nat_tx_name = queryresults[0]
            nat_tx_rate = queryresults[1]

       

        #create a customer instance
    

        cst = Customer(
            CDC_FLAG = row.CDC_FLAG,
            SK_CustomerID = row.CDC_DSN,
            CustomerID = row.C_ID,
            TaxID = row.C_TAX_ID,
            Status = st_name,
            LastName = row.C_L_NAME,
            FirstName = row.C_F_NAME,
            MiddleInitial = row.C_M_NAME,
            Gender = row.C_GNDR,
            Tier = row.C_TIER,
            DOB = row.C_DOB,
            AddressLine1 = row.C_ADLINE1,
            AddressLine2 = row.C_ADLINE2,
            PostalCode = row.C_ZIPCODE,
            City = row.C_CITY,
            StateProv = row.C_STATE_PROV,
            Country = row.C_CTRY,
            Phone1 = Phone1,
            Phone2 = Phone2,
            Phone3 = Phone3,
            Email1 = row.C_EMAIL_1,
            Email2 = row.C_EMAIL_2,
            NationalTaxRateDesc = nat_tx_name,
            NationalTaxRate = nat_tx_rate,
            LocalTaxRateDesc = lcl_tx_name,
            LocalTaxRate = lcl_tx_rate,
            AgencyID = agency_id,
            CreditRating = credit_rating,
            NetWorth = net_worth,
            MarketingNameplate = marketing_nameplate,
            IsCurrent = True,
            BatchID = 2,
            EffectiveDate = date.today(),
            EndDate = date(9999, 12, 31)
        )      
        customers.append(cst)



            
    
    return customers


# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_customer(row: Customer):
    sql_for_insert = f'''
               INSERT INTO DimCustomer (
                SK_CustomerID,
                CustomerID,
                TaxID,
                Status,
                LastName,
                FirstName,
                MiddleInitial,
                Gender,
                Tier,
                DOB,
                AddressLine1,
                AddressLine2,
                PostalCode,
                City,
                StateProv,
                Country,
                Phone1,
                Phone2,
                Phone3,
                Email1,
                Email2,
                NationalTaxRateDesc,
                NationalTaxRate,
                LocalTaxRateDesc,
                LocalTaxRate,
                AgencyID,
                CreditRating,
                NetWorth,
                MarketingNameplate,
                IsCurrent,
                BatchID,
                EffectiveDate,
                EndDate
            ) VALUES (
             {row.SK_CustomerID},
                {row.CustomerID},
                {row.TaxID},
                {row.Status},
                {row.LastName},
                {row.FirstName},
                {row.MiddleInitial},
                {row.Gender},
                {row.Tier},
                {row.DOB},
                {row.AddressLine1},
                {row.AddressLine2},
                {row.PostalCode},
                {row.City},
                {row.StateProv},
                {row.Country},
                {row.Phone1},
                {row.Phone2},
                {row.Phone3},
                {row.Email1},
                {row.Email2},
                {row.NationalTaxRateDesc},
                {row.NationalTaxRate},
                {row.LocalTaxRateDesc},
                {row.LocalTaxRate},
                {row.AgencyID},
                {row.CreditRating},
                {row.NetWorth},
                {row.MarketingNameplate},
                {row.IsCurrent},
                {row.BatchID},
                {row.EffectiveDate},
                {row.EndDate}
            );

        '''
    client_redshift.execute_statement(
        Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    


    # sql_for_insert = '''
    #             INSERT INTO DimCustomer (
    #             SK_CustomerID,
    #             CustomerID,
    #             TaxID,
    #             Status,
    #             LastName,
    #             FirstName,
    #             MiddleInitial,
    #             Gender,
    #             Tier,
    #             DOB,
    #             AddressLine1,
    #             AddressLine2,
    #             PostalCode,
    #             City,
    #             StateProv,
    #             Country,
    #             Phone1,
    #             Phone2,
    #             Phone3,
    #             Email1,
    #             Email2,
    #             NationalTaxRateDesc,
    #             NationalTaxRate,
    #             LocalTaxRateDesc,
    #             LocalTaxRate,
    #             AgencyID,
    #             CreditRating,
    #             NetWorth,
    #             MarketingNameplate,
    #             IsCurrent,
    #             BatchID,
    #             EffectiveDate,
    #             EndDate
    #         ) VALUES (
    #             %(SK_CustomerID)s,
    #             %(CustomerID)s,
    #             %(TaxID)s,
    #             %(Status)s,
    #             %(LastName)s,
    #             %(FirstName)s,
    #             %(MiddleInitial)s,
    #             %(Gender)s,
    #             %(Tier)s,
    #             %(DOB)s,
    #             %(AddressLine1)s,
    #             %(AddressLine2)s,
    #             %(PostalCode)s,
    #             %(City)s,
    #             %(StateProv)s,
    #             %(Country)s,
    #             %(Phone1)s,
    #             %(Phone2)s,
    #             %(Phone3)s,
    #             %(Email1)s,
    #             %(Email2)s,
    #             %(NationalTaxRateDesc)s,
    #             %(NationalTaxRate)s,
    #             %(LocalTaxRateDesc)s,
    #             %(LocalTaxRate)s,
    #             %(AgencyID)s,
    #             %(CreditRating)s,
    #             %(NetWorth)s,
    #             %(MarketingNameplate)s,
    #             %(IsCurrent)s,
    #             %(BatchID)s,
    #             %(EffectiveDate)s,
    #             %(EndDate)s
    #         );
    #     '''
    # cur.execute(sql_for_insert, {
    #             'SK_CustomerID': row.SK_CustomerID,  # Update with the actual value
    #             'CustomerID': row.CustomerID,
    #             'TaxID': row.TaxID,
    #             'Status': row.Status,
    #             'LastName': row.LastName,
    #             'FirstName': row.FirstName,
    #             'MiddleInitial': row.MiddleInitial,
    #             'Gender': row.Gender,
    #             'Tier': row.Tier,
    #             'DOB': row.DOB,
    #             'AddressLine1': row.AddressLine1,
    #             'AddressLine2': row.AddressLine2,
    #             'PostalCode': row.PostalCode,
    #             'City': row.City,
    #             'StateProv': row.StateProv,
    #             'Country': row.Country,
    #             'Phone1': row.Phone1,
    #             'Phone2': row.Phone2,
    #             'Phone3': row.Phone3,
    #             'Email1': row.Email1,
    #             'Email2': row.Email2,
    #             'NationalTaxRateDesc': row.NationalTaxRateDesc,
    #             'NationalTaxRate': row.NationalTaxRate,
    #             'LocalTaxRateDesc': row.LocalTaxRateDesc,
    #             'LocalTaxRate': row.LocalTaxRate,
    #             'AgencyID': row.AgencyID,
    #             'CreditRating': row.CreditRating,
    #             'NetWorth': row.NetWorth,
    #             'MarketingNameplate': row.MarketingNameplate,
    #             'IsCurrent': True,  # Assuming it should be True for new records
    #             'BatchID': 2,
    #             'EffectiveDate': date.today(),
    #             'EndDate': date(9999, 12, 31)
    #         })

    # conn.commit()
    print("Record inserted successfully into DimCustomer table")
def load(rows: list[Customer]):

    for row in rows:

        if(row.CDC_FLAG == 'I'):
            #insert operation
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999
            insert_customer(row)

            
        elif(row.CDC_FLAG == 'U'):
            #update operation
            # Update statement for the record with the matching C_ID with field IsCurrent = True, set the EndDate field to the effective date of the update, IsCurrent = False, and IsActive = False
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999
            

            sql_for_sk_of_old_record = f"SELECT SK_CustomerID FROM public.DimCustomer WHERE CustomerID = {row.CustomerID} AND IsCurrent = True;"
            #sql_for_sk_of_old_record = sql.SQL("SELECT SK_CustomerID FROM public.DimCustomer WHERE CustomerID = %s AND IsCurrent = True;")
            #cur.execute(sql_for_sk_of_old_record, (row.CustomerID,))
            #queryresults = cur.fetchone()
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_of_old_record)
            queryresults = client_redshift.fetchone()

            
            
            sk_of_old_record = queryresults[0]
            

            sql_for_update = f'''

                UPDATE DimCustomer
                SET IsCurrent = False,
                    EndDate = {date.today()}
                WHERE CustomerID = {row.CustomerID}
                    AND IsCurrent = True;
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            

            # sql_for_update = '''
            #     UPDATE DimCustomer
            #     SET IsCurrent = False,
            #         EndDate = %(EffectiveDate)s
            #     WHERE CustomerID = %(CustomerID)s
            #         AND IsCurrent = True;
            # '''
            # cur.execute(sql_for_update, {
            #     'CustomerID': row.CustomerID,
            #     'EffectiveDate': date.today()
            # })

            
            
           # conn.commit()
            
            #insert the new record

            insert_customer(row) # what if update record doesnt mach any record in the table? TODO
            
            print("old customer sk", sk_of_old_record)
            print("new customer sk", row.SK_CustomerID)
            #update all the accounts with old sk with the new sk

            sql_for_update_accounts = f'''
                UPDATE DimAccount
                SET SK_CustomerID = {row.SK_CustomerID}, status = {row.Status}
                WHERE SK_CustomerID = {sk_of_old_record};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_accounts)
            

            # sql_for_update_accounts = '''
            #     UPDATE DimAccount
            #     SET SK_CustomerID = %(SK_CustomerID)s, status = %(Status)s
            #     WHERE SK_CustomerID = %(sk_of_old_record)s;
            # '''
            # cur.execute(sql_for_update_accounts, {
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'Status': row.Status,
            #     'sk_of_old_record': sk_of_old_record
            # })
            #conn.commit()

            #update factwatches with old sk with the new sk


            sql_for_update_factwatches = f'''
                UPDATE FactWatches
                SET SK_CustomerID = {row.SK_CustomerID}
                WHERE SK_CustomerID = {sk_of_old_record};
            '''

            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_factwatches)

            # sql_for_update_factwatches = '''
            #     UPDATE FactWatches
            #     SET SK_CustomerID = %(SK_CustomerID)s
            #     WHERE SK_CustomerID = %(sk_of_old_record)s;
            # '''
            # cur.execute(sql_for_update_factwatches, {
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'sk_of_old_record': sk_of_old_record
            # })

            #update dimtrade with old sk with the new sk

            sql_for_update_dimtrade = f'''
                UPDATE DimTrade
                SET SK_CustomerID = {row.SK_CustomerID}
                WHERE SK_CustomerID = {sk_of_old_record};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_dimtrade)
            

            # sql_for_update_dimtrade = '''
            #     UPDATE DimTrade
            #     SET SK_CustomerID = %(SK_CustomerID)s
            #     WHERE SK_CustomerID = %(sk_of_old_record)s;
            # '''
            # cur.execute(sql_for_update_dimtrade, {
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'sk_of_old_record': sk_of_old_record
            # })

            #update factholding with old sk with the new sk
            sql_for_update_factholding = f'''
                UPDATE FactHolding
                SET SK_CustomerID = {row.SK_CustomerID}
                WHERE SK_CustomerID = {sk_of_old_record};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_factholding)
            

            # sql_for_update_factholding = '''
            #     UPDATE FactHolding
            #     SET SK_CustomerID = %(SK_CustomerID)s
            #     WHERE SK_CustomerID = %(sk_of_old_record)s;
            # '''
            # cur.execute(sql_for_update_factholding, {
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'sk_of_old_record': sk_of_old_record
            # })

            #update factcashbalances with old sk with the new sk
            sql_for_update_factcashbalances = f'''
                UPDATE FactCashBalances
                SET SK_CustomerID = {row.SK_CustomerID}
                WHERE SK_CustomerID = {sk_of_old_record};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_factcashbalances)
            
            
            # sql_for_update_factcashbalances = '''
            #     UPDATE FactCashBalances
            #     SET SK_CustomerID = %(SK_CustomerID)s
            #     WHERE SK_CustomerID = %(sk_of_old_record)s;
            # '''
            # cur.execute(sql_for_update_factcashbalances, {
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'sk_of_old_record': sk_of_old_record
            # })
            
            # conn.commit()

    
            print("Record updated successfully into DimCustomer table")
            
    
        

            
  
        
    # customer_increment_stmt = """
    # """
    
    # try:
    #     client_redshift.execute_statement(
    #         Database='dev', WorkgroupName='tpcdi-benchmark', Sql=create_table_stmt)
    #     client_redshift.execute_statement(
    #         Database='dev', WorkgroupName='tpcdi-benchmark', Sql=delete_stmt)
        
    #     print("deleted")
    #     for chunk in batched(rows, 150):
    #         insert_stmt = f"""
    #         INSERT INTO public.DateDim
    #         VALUES (%s);
    #         """ % str(tuple(tuple([*st]) for st in chunk))
    #         client_redshift.execute_statement(
    #         Database='dev', WorkgroupName='tpcdi-benchmark', Sql=insert_stmt)
    #     print("API successfully executed")
        
    # except botocore.exceptions.ConnectionError as e:
    #     print("API executed after reestablishing the connection")
    #     return str(result)
        
    # except Exception as e:
    #     raise Exception(e)
    # return 200

def lambda_handler(event, context):
    raw_rows = extract()
    rows = transform(raw_rows)
    
    # for row in rows:
    #     print(row)

    response = load(rows)
        
    return str(response)


lambda_handler(0,0)