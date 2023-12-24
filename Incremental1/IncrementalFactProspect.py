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
from datetime import datetime

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

#class Account(NamedTuple):

class FactProspect(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    AgencyID: str
    SK_RecordDateID: int
    SK_UpdateDateID: int
    BatchID: int
    IsCustomer: bool
    LastName: str
    FirstName: str
    MiddleInitial: str
    Gender: str
    AddressLine1: str
    AddressLine2: str
    PostalCode: str
    City: str
    State: str
    Country: str
    Phone: str
    Income: int
    NumberCars: int
    NumberChildren: int
    MaritalStatus: str
    Age: int
    CreditRating: int
    OwnOrRentFlag: str
    Employer: str
    NumberCreditCards: int
    NetWorth: int
    MarketingNameplate: str

class FactProspectIncremental(NamedTuple):
    AgencyID: str
    LastName: str
    FirstName: str
    MiddleInitial: str
    Gender: str
    AddressLine1: str
    AddressLine2: str
    PostalCode: str
    City: str
    State: str
    Country: str
    Phone: str
    Income: int
    NumberCars: int
    NumberChildren: int
    MaritalStatus: str
    Age: int
    CreditRating: int
    OwnOrRentFlag: str
    Employer: str
    NumberCreditCards: int
    NetWorth: int

    


def parse_prospect_incremental(line: str) -> FactProspectIncremental:
    values = line.strip().split(',')
    
    return FactProspectIncremental(
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
        Income= int(values[12]) if values[12] else None,
        NumberCars= int(values[13]) if values[13] else None,
        NumberChildren= int(values[14]) if values[14] else None,
        MaritalStatus=values[15],
        Age= int(values[16]) if values[16] else None,
        CreditRating= int(values[17]) if values[17] else None,
        OwnOrRentFlag=values[18],
        Employer=values[19],
        NumberCreditCards= int(values[20]) if values[20] else None,
        NetWorth= int(values[21]) if values[21] else None

    )

def extract() -> list[FactProspectIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/Prospect.csv',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/Prospect2.csv', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_prospect_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):



    
    
    


    factProspects = []



    for row in raw_rows:
        
        #check for agency id in the prospect table to see if it exists
        sql_for_agency_id = f"SELECT AgencyID FROM public.Prospect WHERE AgencyID = '{row.AgencyID}';"

        #sql_for_agency_id = sql.SQL("SELECT AgencyID FROM public.Prospect WHERE AgencyID = %s;")
        #cur.execute(sql_for_agency_id, (row.AgencyID,))
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_agency_id
        )

        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()
        agency_id = None
        if queryresults:
            agency_id = queryresults[0]
        
        cdc_flag = "I"
        if(agency_id):
            cdc_flag = "U"
        #get the batch date from the batch date file
        response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/BatchDate.txt',
        )
        cust_txt_string = response['Body'].read().decode('utf-8')
        # cust_txt_string = ""
        # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/BatchDate2.txt', 'r') as f:
        #     cust_txt_string = f.read()
        
        lines = cust_txt_string.splitlines()
        #print file content
        batch_date = ""
        for line in lines:
            stripped_line = line.strip()
            batch_date = stripped_line
        print(batch_date)
        #get the sk_record_date_id from the date dimension table

        sql_for_sk_record_date_id = f"SELECT SK_DateID FROM public.DimDate WHERE datevalue = '{batch_date}';"


        # sql_for_sk_record_date_id = sql.SQL("SELECT SK_DateID FROM public.DimDate WHERE datevalue = %s;")
        # cur.execute(sql_for_sk_record_date_id, (batch_date,))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_record_date_id
        )

        # Fetch all the rows

        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()
        sk_record_date_id = None

        if queryresults:
            sk_record_date_id = queryresults[0]
        print(sk_record_date_id)

        
        # SK_UpdateDateID is set to the DimDate SK_DateID field that Batch Date if this is the first
        # time this AgencyID value has appeared in the Prospect file or if this AgencyID value has
        # appeared before and the values of any of the following fields are different from prior
        # saved values for the same AgencyID value in the Prospects table: LastName, FirstName,
        # MiddleInitial, Gender, AddressLine1, AddressLine2, PostalCode, City, State, Country,
        # Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating,
        # OwnOrRentFlag, Employer, NumberCreditCards, NetWorth. Otherwise, SK_UpdateDateID
        # retains its prior saved value

        #check if the agency id exists in the prospect table
        sql_for_agency_id = f"SELECT SK_UpdateDateID FROM public.Prospect WHERE AgencyID = '{row.AgencyID}';"
        # sql_for_agency_id = sql.SQL("SELECT SK_UpdateDateID FROM public.Prospect WHERE AgencyID = %s;")
        # cur.execute(sql_for_agency_id, (row.AgencyID,))
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_agency_id
        )
        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()
       
        sk_update_date_id = None
        if queryresults:
            
            sk_update_date_id = queryresults[0] 
        
        if not sk_update_date_id:
            sk_update_date_id = sk_record_date_id
        else:
            #check if the values of any of the following fields are different from prior
            # saved values for the same AgencyID value in the Prospects table: LastName, FirstName,
            sql_for_check = f"SELECT LastName, FirstName, MiddleInitial,Gender, AddressLine1, AddressLine2, PostalCode, City, State, Country, Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating, OwnOrRentFlag, Employer, NumberCreditCards, NetWorth,SK_UpdateDateID FROM public.Prospect WHERE AgencyID = '{row.AgencyID}';"

            #sql_for_check = "SELECT LastName, FirstName, MiddleInitial,Gender, AddressLine1, AddressLine2, PostalCode, City, State, Country, Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating, OwnOrRentFlag, Employer, NumberCreditCards, NetWorth,SK_UpdateDateID FROM public.Prospect WHERE AgencyID = %s;"
            #cur.execute(sql_for_check, (row.AgencyID,))
            
            client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_check
            )
            queryresults = client_redshift.fetchone()

            #queryresults = cur.fetchone()
            if queryresults:
                lastname = queryresults[0]
                firstname = queryresults[1]
                middleinitial = queryresults[2]
                gender = queryresults[3]
                addressline1 = queryresults[4]
                addressline2 = queryresults[5]
                postalcode = queryresults[6]
                city = queryresults[7]
                state = queryresults[8]
                country = queryresults[9]
                phone = queryresults[10]
                income = queryresults[11]
                numbercars = queryresults[12]
                numberchildren = queryresults[13]
                maritalstatus = queryresults[14]
                age = queryresults[15]
                creditrating = queryresults[16]
                ownorrentflag = queryresults[17]
                employer = queryresults[18]
                numbercreditcards = queryresults[19]
                networth = queryresults[20]
                last_update_id = queryresults[21]

                if(lastname != row.LastName or firstname != row.FirstName or middleinitial != row.MiddleInitial or gender != row.gender
                   or addressline1 != row.AddressLine1 or addressline2 != row.AddressLine2 or postalcode != row.PostalCode or city != row.City
                   or state != row.State or country != row.Country or phone != row.Phone or income != row.Income or numbercars != row.NumberCars
                   or numberchildren != row.NumberChildren or maritalstatus != row.MaritalStatus or age != row.Age or creditrating != row.CreditRating
                   or ownorrentflag != row.OwnOrRentFlag or employer != row.Employer or numbercreditcards != row.NumberCreditCards or networth != row.NetWorth):
                    sk_update_date_id = sk_record_date_id
                else:
                    sk_update_date_id = last_update_id
            
        print(sk_update_date_id)

        # IsCustomer is set to True or False depending on whether the prospective customer record
        # matches a current customer record in DimCustomer whose status is ‘ACTIVE’ after all
        # customer records in the batch have been processed. A Prospect record is deemed to
        # match a DimCustomer record if the FirstName, LastName, AddressLine1, AddressLine2
        # and PostalCode fields all match when upper-cased.

        sql_for_iscustomer = f"SELECT iscurrent FROM public.DimCustomer WHERE UPPER(FirstName) = '{row.FirstName.upper()}' AND UPPER(LastName) = '{row.LastName.upper()}' AND UPPER(AddressLine1) = '{row.AddressLine1.upper()}' AND UPPER(AddressLine2) = '{row.AddressLine2.upper()}' AND UPPER(PostalCode) = '{row.PostalCode.upper()}' AND Status = 'ACTIVE';"
        #sql_for_iscustomer = sql.SQL("SELECT iscurrent FROM public.DimCustomer WHERE UPPER(FirstName) = %s AND UPPER(LastName) = %s AND UPPER(AddressLine1) = %s AND UPPER(AddressLine2) = %s AND UPPER(PostalCode) = %s AND Status = 'ACTIVE';")

        #cur.execute(sql_for_iscustomer, (row.FirstName.upper(), row.LastName.upper(), row.AddressLine1.upper(), row.AddressLine2.upper(), row.PostalCode.upper()))
        
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_iscustomer
        )
        #queryresults = cur.fetchone()
        queryresults = client_redshift.fetchone()
        iscustomer = False
        if queryresults:
            iscustomer = queryresults[0]
        print(iscustomer)


        tags = []

        # Check conditions for each tag and append to the list
        
        if row.NetWorth != None and row.Income != None and (row.NetWorth > 1000000 or row.Income > 200000):
            tags.append("HighValue")

        if row.NumberChildren != None and row.NumberCreditCards != None and (row.NumberChildren > 3 or row.NumberCreditCards > 5):
            tags.append("Expenses")

        if row.Age != None and  row.Age > 45:
            tags.append("Boomer")

        if row.Income != None and row.CreditRating != None and  row.NetWorth != None and (row.Income < 50000 or row.CreditRating < 600 or row.NetWorth < 100000):
            tags.append("MoneyAlert")

        if row.NumberCars != None and row.NumberCreditCards != None and (row.NumberCars > 3 or row.NumberCreditCards > 7):
            tags.append("Spender")

        if row.Age != None and row.NetWorth != None and (row.Age < 25 and row.NetWorth > 1000000):
            tags.append("Inherited")

        # Concatenate tags with '+' or return None if no tags apply
        marketing_nameplate =  '+'.join(tags) if tags else None


        

        factProspects.append(FactProspect(
            CDC_FLAG=cdc_flag,
            AgencyID=row.AgencyID,
            SK_RecordDateID=sk_record_date_id,
            SK_UpdateDateID=sk_update_date_id,
            BatchID=2,
            IsCustomer=iscustomer,
            LastName=row.LastName,
            FirstName=row.FirstName,
            MiddleInitial=row.MiddleInitial,
            Gender= row.Gender,
            AddressLine1=row.AddressLine1,
            AddressLine2=row.AddressLine2,
            PostalCode=row.PostalCode,
            City=row.City,
            State=row.State,
            Country=row.Country,
            Phone=row.Phone,
            Income=row.Income,
            NumberCars=row.NumberCars,
            NumberChildren=row.NumberChildren,
            MaritalStatus=row.MaritalStatus,
            Age=row.Age,
            CreditRating=row.CreditRating,
            OwnOrRentFlag=row.OwnOrRentFlag,
            Employer=row.Employer,
            NumberCreditCards=row.NumberCreditCards,
            NetWorth=row.NetWorth,
            MarketingNameplate=marketing_nameplate

            )
        )

            
    
    return factProspects

# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_prospect(row: FactProspect):
    sql_for_insert = f"""
        INSERT INTO public.Prospect (AgencyID,SK_RecordDateID,SK_UpdateDateID, BatchID, IsCustomer, LastName, FirstName, MiddleInitial, Gender,AddressLine1, AddressLine2, PostalCode, City, State, Country, Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating, OwnOrRentFlag, Employer, NumberCreditCards, NetWorth, MarketingNameplate)
        VALUES ({row.AgencyID},{row.SK_RecordDateID},{row.SK_UpdateDateID},{row.BatchID},{row.IsCustomer},{row.LastName},{row.FirstName},{row.MiddleInitial},{row.Gender},{row.AddressLine1},{row.AddressLine2},{row.PostalCode},{row.City},{row.State},{row.Country},{row.Phone},{row.Income},{row.NumberCars},{row.NumberChildren},{row.MaritalStatus},{row.Age},{row.CreditRating},{row.OwnOrRentFlag},{row.Employer},{row.NumberCreditCards},{row.NetWorth},{row.MarketingNameplate});
    """
    
    client_redshift.execute_statement(
        Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    

    # sql_for_insert = '''
    #     INSERT INTO public.Prospect (AgencyID,SK_RecordDateID,SK_UpdateDateID, BatchID, IsCustomer, LastName, FirstName, MiddleInitial, Gender,AddressLine1, AddressLine2, PostalCode, City, State, Country, Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating, OwnOrRentFlag, Employer, NumberCreditCards, NetWorth, MarketingNameplate)
    #     VALUES (%(AgencyID)s,%(SK_RecordDateID)s,%(SK_UpdateDateID)s,%(BatchID)s,%(IsCustomer)s,%(LastName)s,%(FirstName)s, %(MiddleInitial)s, %(Gender)s, %(AddressLine1)s, %(AddressLine2)s, %(PostalCode)s, %(City)s, %(State)s, %(Country)s, %(Phone)s, %(Income)s, %(NumberCars)s, %(NumberChildren)s, %(MaritalStatus)s, %(Age)s, %(CreditRating)s, %(OwnOrRentFlag)s, %(Employer)s, %(NumberCreditCards)s, %(NetWorth)s, %(MarketingNameplate)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #     'AgencyID' : row.AgencyID,
    #     'SK_RecordDateID' : row.SK_RecordDateID,
    #     'SK_UpdateDateID' : row.SK_UpdateDateID,
    #     'BatchID' : row.BatchID,
    #     'IsCustomer' : row.IsCustomer,
    #     'LastName' : row.LastName,
    #     'FirstName' : row.FirstName,
    #     'MiddleInitial' : row.MiddleInitial,
    #     'Gender': row.Gender,
    #     'AddressLine1': row.AddressLine1,
    #     'AddressLine2': row.AddressLine2,
    #     'PostalCode': row.PostalCode,
    #     'City': row.City,
    #     'State': row.State,
    #     'Country': row.Country,
    #     'Phone': row.Phone,
    #     'Income': row.Income,
    #     'NumberCars': row.NumberCars,
    #     'NumberChildren': row.NumberChildren,
    #     'MaritalStatus': row.MaritalStatus,
    #     'Age': row.Age,
    #     'CreditRating': row.CreditRating,
    #     'OwnOrRentFlag': row.OwnOrRentFlag,
    #     'Employer': row.Employer,
    #     'NumberCreditCards': row.NumberCreditCards,
    #     'NetWorth': row.NetWorth,
    #     'MarketingNameplate': row.MarketingNameplate


    #         })

    # conn.commit()

def load(rows: list[FactProspect]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_prospect(row)
            print("Record inserted successfully into Prospect table")

        elif(row.CDC_FLAG == 'U'):
            
            #update the existing record with the same agency id
            
            sql_for_update = f"""
                UPDATE public.Prospect
                SET SK_RecordDateID = {row.SK_RecordDateID}, SK_UpdateDateID = {row.SK_UpdateDateID}, BatchID = {row.BatchID}, IsCustomer = {row.IsCustomer}, LastName = {row.LastName}, FirstName = {row.FirstName}, MiddleInitial = {row.MiddleInitial}, Gender = {row.Gender}, AddressLine1 = {row.AddressLine1}, AddressLine2 = {row.AddressLine2}, PostalCode = {row.PostalCode}, City = {row.City}, State = {row.State}, Country = {row.Country}, Phone = {row.Phone}, Income = {row.Income}, NumberCars = {row.NumberCars}, NumberChildren = {row.NumberChildren}, MaritalStatus = {row.MaritalStatus}, Age = {row.Age}, CreditRating = {row.CreditRating}, OwnOrRentFlag = {row.OwnOrRentFlag}, Employer = {row.Employer}, NumberCreditCards = {row.NumberCreditCards}, NetWorth = {row.NetWorth}, MarketingNameplate = {row.MarketingNameplate}
                WHERE AgencyID = {row.AgencyID};
            """

            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            

            # sql_for_update = '''
            # UPDATE public.Prospect
            # SET SK_RecordDateID = %(SK_RecordDateID)s,SK_UpdateDateID = %(SK_UpdateDateID)s, BatchID = %(BatchID)s, IsCustomer = %(IsCustomer)s, LastName = %(LastName)s, FirstName = %(FirstName)s, MiddleInitial = %(MiddleInitial)s, Gender = %(Gender)s, AddressLine1 = %(AddressLine1)s, AddressLine2 = %(AddressLine2)s, PostalCode = %(PostalCode)s, City = %(City)s, State = %(State)s, Country = %(Country)s, Phone = %(Phone)s, Income = %(Income)s, NumberCars = %(NumberCars)s, NumberChildren = %(NumberChildren)s, MaritalStatus = %(MaritalStatus)s, Age = %(Age)s, CreditRating = %(CreditRating)s, OwnOrRentFlag = %(OwnOrRentFlag)s, Employer = %(Employer)s, NumberCreditCards = %(NumberCreditCards)s, NetWorth = %(NetWorth)s, MarketingNameplate = %(MarketingNameplate)s
            # WHERE AgencyID = %(AgencyID)s;
            # '''

            # cur.execute(sql_for_update, {
            #     'AgencyID' : row.AgencyID,
            #     'SK_RecordDateID' : row.SK_RecordDateID,
            #     'SK_UpdateDateID' : row.SK_UpdateDateID,
            #     'BatchID' : row.BatchID,
            #     'IsCustomer' : row.IsCustomer,
            #     'LastName' : row.LastName,
            #     'FirstName' : row.FirstName,
            #     'MiddleInitial' : row.MiddleInitial,
            #     'Gender': row.Gender,
            #     'AddressLine1': row.AddressLine1,
            #     'AddressLine2': row.AddressLine2,
            #     'PostalCode': row.PostalCode,
            #     'City': row.City,
            #     'State': row.State,
            #     'Country': row.Country,
            #     'Phone': row.Phone,
            #     'Income': row.Income,
            #     'NumberCars': row.NumberCars,
            #     'NumberChildren': row.NumberChildren,
            #     'MaritalStatus': row.MaritalStatus,
            #     'Age': row.Age,
            #     'CreditRating': row.CreditRating,
            #     'OwnOrRentFlag': row.OwnOrRentFlag,
            #     'Employer': row.Employer,
            #     'NumberCreditCards': row.NumberCreditCards,
            #     'NetWorth': row.NetWorth,
            #     'MarketingNameplate': row.MarketingNameplate
            # }
            # )
            # conn.commit()



        
            print("Record updated successfully into Prospect table")
            
    return 0
        

            
  
        
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