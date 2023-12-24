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

#class Account(NamedTuple):

class Account(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    SK_AccountID: int  # Surrogate key for AccountID
    AccountID: str  # Not NULL Customer account identifier
    SK_BrokerID: int  # Surrogate key of managing broker
    SK_CustomerID: int  # Surrogate key of customer
    Status: str  # Not NULL Account status, active or closed
    AccountDesc: str  # Name of customer account
    TaxStatus: int  # 0, 1, or 2 Tax status of this account
    IsCurrent: bool  # Not NULL True if this is the current record
    BatchID: int  # Not NULL Batch ID when this record was inserted
    EffectiveDate: str  # Not NULL Beginning of date range when this record was the current record
    EndDate: str  # Not NULL Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.

class AccountIncremental(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    CDC_DSN: int  # Not NULL Database Sequence Number
    CA_ID: str  # Not NULL Customer account identifier
    CA_B_ID: str  # Not NULL Identifier of the managing broker
    CA_C_ID: str  # Not NULL Owning customer identifier
    CA_NAME: str  # Name of customer account
    CA_TAX_ST: int  # 0, 1, or 2 Tax status of this account
    CA_ST_ID: str  # 'ACTV' or 'INAC' Customer status type identifier



def parse_account_incremental(line: str) -> AccountIncremental:
    values = line.strip().split('|')
    
    return AccountIncremental(

        CDC_FLAG=values[0],
        CDC_DSN=values[1],
        CA_ID=values[2],
        CA_B_ID=values[3],
        CA_C_ID=values[4],
        CA_NAME=values[5],
        CA_TAX_ST=values[6],
        CA_ST_ID=values[7]
    )

def extract() -> list[AccountIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/Account.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/Account2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_account_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):

    accounts = []

    for row in raw_rows:
        
 
        sql_for_st_name = f"SELECT ST_NAME FROM public.StatusType WHERE ST_ID = {row.CA_ST_ID};"
        #sql_for_st_name = sql.SQL("SELECT ST_NAME FROM public.StatusType WHERE ST_ID = %s;")
        
        #cur.execute(sql_for_st_name, (row.CA_ST_ID,))

        # Fetch all the rows
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_st_name)
        queryresults = client_redshift.fetchone()


        st_name = queryresults[0]


        sql_for_sk_broker = f"SELECT SK_BrokerID FROM public.DimBroker WHERE BrokerID = {row.CA_B_ID} and IsCurrent = True;"
        #sql_for_sk_broker = sql.SQL("SELECT SK_BrokerID FROM public.DimBroker WHERE BrokerID = %s and IsCurrent = True;")

        #cur.execute(sql_for_sk_broker, (row.CA_B_ID,))
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_broker)
        queryresults = client_redshift.fetchone()

        
        print(queryresults)
        sk_broker = queryresults[0]

        sql_for_sk_customer = f"SELECT SK_CustomerID FROM public.DimCustomer WHERE CustomerID = {row.CA_C_ID} and IsCurrent = True;"
        #sql_for_sk_customer = sql.SQL("SELECT SK_CustomerID FROM public.DimCustomer WHERE CustomerID = %s and IsCurrent = True;")

        #cur.execute(sql_for_sk_customer, (row.CA_C_ID,))    
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_customer)
        queryresults = client_redshift.fetchone()

        
        print(queryresults)
        sk_customer = queryresults[0]

        account = Account(
            CDC_FLAG = row.CDC_FLAG,
            SK_AccountID = row.CDC_DSN,
            AccountID = row.CA_ID,
            SK_BrokerID = sk_broker,
            SK_CustomerID = sk_customer,
            Status = st_name,
            AccountDesc = row.CA_NAME,
            TaxStatus = row.CA_TAX_ST,
            IsCurrent = True,
            BatchID = 2,
            EffectiveDate = date.today(),
            EndDate = date(9999, 12, 31)
        )
        accounts.append(account)
        
            
    
    return accounts


# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_account(row: Account):

    sql_for_insert = f"""
    INSERT INTO DimAccount (SK_AccountID, AccountID, SK_BrokerID, SK_CustomerID, Status, AccountDesc, TaxStatus, IsCurrent, BatchID, EffectiveDate, EndDate)
    VALUES ({row.SK_AccountID}, {row.AccountID}, {row.SK_BrokerID}, {row.SK_CustomerID}, '{row.Status}', '{row.AccountDesc}', {row.TaxStatus}, {row.IsCurrent}, {row.BatchID}, '{row.EffectiveDate}', '{row.EndDate}');
    """
    client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    

    # sql_for_insert = '''
    #     INSERT INTO DimAccount (SK_AccountID, AccountID, SK_BrokerID, SK_CustomerID, Status, AccountDesc, TaxStatus, IsCurrent, BatchID, EffectiveDate, EndDate)    
    #     VALUES (%(SK_AccountID)s, %(AccountID)s, %(SK_BrokerID)s, %(SK_CustomerID)s, %(Status)s, %(AccountDesc)s, %(TaxStatus)s, %(IsCurrent)s, %(BatchID)s, %(EffectiveDate)s, %(EndDate)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #             'SK_AccountID': row.SK_AccountID,
    #             'AccountID': row.AccountID,
    #             'SK_BrokerID': row.SK_BrokerID,
    #             'SK_CustomerID': row.SK_CustomerID,
    #             'Status': row.Status,
    #             'AccountDesc': row.AccountDesc,
    #             'TaxStatus': row.TaxStatus,
    #             'IsCurrent': row.IsCurrent,
    #             'BatchID': row.BatchID,
    #             'EffectiveDate': row.EffectiveDate,
    #             'EndDate': row.EndDate
    #         })

    # conn.commit()
    print("Record inserted successfully into DimAccount table")
def load(rows: list[Account]):

    for row in rows:

        if(row.CDC_FLAG == 'I'):
            #insert operation
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999
            insert_account(row)

            
        elif(row.CDC_FLAG == 'U'):
            #update operation
            # Update statement for the record with the matching C_ID with field IsCurrent = True, set the EndDate field to the effective date of the update, IsCurrent = False, and IsActive = False
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999
            
            print(row.AccountID)

            sk_for_old_account = f"SELECT SK_AccountID FROM public.DimAccount WHERE AccountID = {row.AccountID} and IsCurrent = True;"

            #sk_for_old_account = sql.SQL("SELECT SK_AccountID FROM public.DimAccount WHERE AccountID = %s and IsCurrent = True;")
            #cur.execute(sk_for_old_account, (row.AccountID,))
            #queryresults = cur.fetchone()
            
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sk_for_old_account)
            queryresults = client_redshift.fetchone()

            
            sk_old_account = queryresults[0]

            sql_for_update = f"""
                UPDATE DimAccount
                SET IsCurrent = False,
                    EndDate = '{date.today()}'
                WHERE AccountID = {row.AccountID}
                    AND IsCurrent = True;
            """
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            

            # sql_for_update = '''
            #     UPDATE DimAccount
            #     SET IsCurrent = False,
            #         EndDate = %(EffectiveDate)s
            #     WHERE AccountID = %(AccountID)s
            #         AND IsCurrent = True;
            # '''
            # cur.execute(sql_for_update, {
            #     'AccountID': row.AccountID,
            #     'EffectiveDate': date.today()
            # })

            


            #update dimTrade table's SK_AccountID field with the new SK_AccountID
            sql_for_update_trade = f'''
                UPDATE DimTrade
                SET SK_AccountID = {row.SK_AccountID}
                WHERE SK_AccountID = {sk_old_account};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_trade)
            
           
            # sql_for_update_trade = '''
            #     UPDATE DimTrade
            #     SET SK_AccountID = %(SK_AccountID)s
            #     WHERE SK_AccountID = %(SK_AccountID_old)s;
            # '''
            # cur.execute(sql_for_update_trade, {
            #     'SK_AccountID': row.SK_AccountID,
            #     'SK_AccountID_old': sk_old_account
            # })

            #update factholdings table's SK_AccountID field with the new SK_AccountID
            sql_for_update_holdings = f'''
                UPDATE FactHoldings
                SET SK_AccountID = {row.SK_AccountID}
                WHERE SK_AccountID = {sk_old_account};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_holdings)
            
            
            # sql_for_update_holdings = '''
            #     UPDATE FactHoldings
            #     SET SK_AccountID = %(SK_AccountID)s
            #     WHERE SK_AccountID = %(SK_AccountID_old)s;
            # '''
            # cur.execute(sql_for_update_holdings, {
            #     'SK_AccountID': row.SK_AccountID,
            #     'SK_AccountID_old': sk_old_account
            # })

            #update factcashbalances table's SK_AccountID field with the new SK_AccountID
            
            sql_for_update_cashbalances = f'''
                UPDATE FactCashBalances
                SET SK_AccountID = {row.SK_AccountID}
                WHERE SK_AccountID = {sk_old_account};
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update_cashbalances)
            
            
            # sql_for_update_cashbalances = '''
            #     UPDATE FactCashBalances
            #     SET SK_AccountID = %(SK_AccountID)s
            #     WHERE SK_AccountID = %(SK_AccountID_old)s;
            # '''
            # cur.execute(sql_for_update_cashbalances, {
            #     'SK_AccountID': row.SK_AccountID,
            #     'SK_AccountID_old': sk_old_account
            # })
            
            

            
            #conn.commit()
            
            #insert the new record

            insert_account(row) # what if update record doesnt mach any record in the table? TODO
            print("Record updated successfully into DimAccount table")
            
    
        

            
  
        
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