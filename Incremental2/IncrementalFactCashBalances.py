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

class FactCashBalances(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    SK_CustomerID: int
    SK_AccountID: int
    SK_DateID: int
    Cash: float
    BatchID: int

class FactCashBalancesIncremental(NamedTuple):
    CDC_FLAG: str  # 'I' Denotes insert
    CDC_DSN: int  # Not NULL Database Sequence Number
    CT_CA_ID: int  # Not Null Customer account identifier
    CT_DTS: datetime  # Not Null Timestamp of when the trade took place
    CT_AMT: float  # Not Null Amount of the cash transaction.
    CT_NAME: str  # Not Null Transaction name, or description

    


def parse_cash_transaction_incremental(line: str) -> FactCashBalancesIncremental:
    values = line.strip().split('|')
    
    return FactCashBalancesIncremental(
        CDC_FLAG=values[0],
        CDC_DSN=values[1],
        CT_CA_ID=values[2],
        CT_DTS=values[3],
        CT_AMT= float(values[4]) if values[4] else None,
        CT_NAME=values[5]

    )

def extract() -> list[FactCashBalancesIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/CashTransaction.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/CashTransaction2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_cash_transaction_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):

    factCashBalances = []



    for row in raw_rows:
        
        
        sql_for_sk_account = f" SELECT SK_AccountID, SK_CustomerID FROM public.DimAccount WHERE AccountID = {row.CT_CA_ID} and IsCurrent = True;"
        #sql_for_sk_account = sql.SQL("SELECT SK_AccountID, SK_CustomerID FROM public.DimAccount WHERE AccountID = %s and IsCurrent = True;")

        #cur.execute(sql_for_sk_account, (row.CT_CA_ID,))
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_account)
        queryresults = client_redshift.fetchone()

        
        #print(queryresults)
        sk_account = None
        sk_customer = None
        if queryresults:
            sk_account = queryresults[0]
            sk_customer = queryresults[1]

        response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/BarchDate.txt',
        )
        cust_txt_string = response['Body'].read().decode('utf-8')

        # cust_txt_string = ""
        # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/BatchDate.txt', 'r') as f:
        #     cust_txt_string = f.read()
        
        lines = cust_txt_string.splitlines()
        #print file content
        batch_date = ""
        for line in lines:
            stripped_line = line.strip()
            batch_date = stripped_line

       

        #Cash is calculated as the sum of the prior Cash amount for this account

        # sql_for_cash = sql.SQL("SELECT SUM(Cash) FROM public.FactCashBalances WHERE SK_AccountID = %s;")

        # cur.execute(sql_for_cash, (sk_account,))
        # queryresults = cur.fetchone()
        # print(queryresults)

        #get the last cash balance for this account
        sql_for_cash = f"SELECT Cash FROM public.FactCashBalances WHERE SK_AccountID = {sk_account} ORDER BY SK_DateID DESC LIMIT 1;"
        # sql_for_cash = sql.SQL("SELECT Cash FROM public.FactCashBalances WHERE SK_AccountID = %s ORDER BY SK_DateID DESC LIMIT 1;")
        # cur.execute(sql_for_cash, (sk_account,))
        # queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_cash)
        queryresults = client_redshift.fetchone()


        cdc_flag = 'U'
        updatedCash = 0.0
        if not queryresults:
            updatedCash = 0.0 + (row.CT_AMT)
            cdc_flag = 'I'
        else:
            updatedCash =  float(queryresults[0]) + (row.CT_AMT)
        print(updatedCash)

    



        

        factCashBalance = FactCashBalances(
            CDC_FLAG = cdc_flag,
            SK_CustomerID = sk_customer,
            SK_AccountID = sk_account,
            SK_DateID = batch_date,
            Cash = updatedCash,
            BatchID = 2
        )
        factCashBalances.append(factCashBalance)
        
            
    
    return factCashBalances


# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_fact_cash_balance(row: FactCashBalances):

    sql_for_insert = f"""
    INSERT INTO FactCashBalances (SK_CustomerID, SK_AccountID, SK_DateID, Cash, BatchID)
    VALUES ({row.SK_CustomerID}, {row.SK_AccountID}, {row.SK_DateID}, {row.Cash}, {row.BatchID});
    """
    client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    

    # sql_for_insert = '''
    #     INSERT INTO FactCashBalances (SK_CustomerID, SK_AccountID, SK_DateID, Cash, BatchID)
    #     VALUES (%(SK_CustomerID)s, %(SK_AccountID)s, %(SK_DateID)s, %(Cash)s, %(BatchID)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #             'SK_CustomerID': row.SK_CustomerID,
    #             'SK_AccountID': row.SK_AccountID,
    #             'SK_DateID': row.SK_DateID,
    #             'Cash': row.Cash,
    #             'BatchID': row.BatchID
    #         })

    # conn.commit()
    print("Record inserted successfully into DimFactCashBalances table")
def load(rows: list[FactCashBalances]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_fact_cash_balance(row)
            print("Record inserted successfully into DimFactCashBalances table")

        elif(row.CDC_FLAG == 'U'):
            #update operation
            # Update statement for the record with the matching C_ID with field IsCurrent = True, set the EndDate field to the effective date of the update, IsCurrent = False, and IsActive = False
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999

            sql_for_update = f"""
                UPDATE FactCashBalances
                SET Cash = {row.Cash}
                WHERE SK_AccountID = {row.SK_AccountID}
                ;
            """
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            

            # sql_for_update = '''
            #     UPDATE FactCashBalances
            #     SET Cash = %(Cash)s
            #     WHERE SK_AccountID = %(SK_AccountID)s
            #     ;
            # '''
            # cur.execute(sql_for_update, {
            #     'Cash': row.Cash,
            #     'SK_AccountID': row.SK_AccountID,
            # })

            
            # conn.commit()
        
            print("Record updated successfully into FactCashBalances table")
            
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
        
    #return str(response)
    return 0

lambda_handler(0,0)