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

# # Replace these with your actual database connection details
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

class FactHoldings(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    TradeID: int
    CurrentTradeID: int
    SK_CustomerID: int
    SK_AccountID: int
    SK_SecurityID: int
    SK_CompanyID: int
    SK_DateID: int
    SK_TimeID: int
    CurrentPrice: float
    CurrentHolding: int
    BatchID: int

class FactHoldingsIncremental(NamedTuple):
  
    CDC_FLAG: str
    CDC_DSN: int
    HH_H_T_ID: int
    HH_T_ID: int
    HH_BEFORE_QTY: int
    HH_AFTER_QTY: int
    


def parse_holding_history_incremental(line: str) -> FactHoldingsIncremental:
    values = line.strip().split('|')
    
    return FactHoldingsIncremental(
        CDC_FLAG=values[0],
        CDC_DSN= int(values[1]) if values[1] else None,
        HH_H_T_ID= int(values[2]) if values[2] else None,
        HH_T_ID=int(values[3]) if values[3] else None,
        HH_BEFORE_QTY= int(values[4]) if values[4] else None,
        HH_AFTER_QTY= int(values[5]) if values[5] else None,

    )

def extract() -> list[FactHoldingsIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/HoldingHistory.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/HoldingHistory2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_holding_history_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):



    
    
    


    factHoldings = []



    for row in raw_rows:
        
 
        #Retrieve the following values from DimTrade where HH_T_ID (current trade identifier) from the HoldingHistory.txt file matches the TradeID from DimTrade
        
        sql_for_retrievals = f"SELECT SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, TradePrice, SK_CloseDateID, SK_CloseTimeID FROM public.DimTrade WHERE TradeID = {row.HH_T_ID};"
        #sql_for_retrievals = sql.SQL("SELECT SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, TradePrice, SK_CloseDateID, SK_CloseTimeID FROM public.DimTrade WHERE TradeID = %s;")
        #cur.execute(sql_for_retrievals, (row.HH_T_ID,))

        # Fetch all the rows
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_retrievals)
        queryresults = client_redshift.fetchone()

        sk_customer = queryresults[0]
        sk_account = queryresults[1]
        sk_security = queryresults[2]
        sk_company = queryresults[3]
        current_price = queryresults[4]
        sk_date = queryresults[5]
        sk_time = queryresults[6]


    
    



        

        factHolding = FactHoldings(
            CDC_FLAG = 'I',
            TradeID = row.HH_T_ID,
            CurrentTradeID = row.HH_H_T_ID,
            SK_CustomerID = sk_customer,
            SK_AccountID = sk_account,
            SK_SecurityID = sk_security,
            SK_CompanyID = sk_company,
            SK_DateID = sk_date,
            SK_TimeID = sk_time,
            CurrentPrice = current_price,
            CurrentHolding = row.HH_AFTER_QTY,
            BatchID = 2
        )
        
        factHoldings.append(factHolding)
        
            
    
    return factHoldings

# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_fact_holding(row: FactHoldings):

    sql_for_insert = f"""
    INSERT INTO FactHoldings (TradeID, CurrentTradeID, SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, SK_DateID, SK_TimeID, CurrentPrice, CurrentHolding, BatchID)
    VALUES ({row.TradeID}, {row.CurrentTradeID}, {row.SK_CustomerID}, {row.SK_AccountID}, {row.SK_SecurityID}, {row.SK_CompanyID}, {row.SK_DateID}, {row.SK_TimeID}, {row.CurrentPrice}, {row.CurrentHolding}, {row.BatchID});
    """
    client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    
    # sql_for_insert = '''
    #     INSERT INTO FactHoldings (TradeID, CurrentTradeID, SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, SK_DateID, SK_TimeID, CurrentPrice, CurrentHolding, BatchID)
    #     VALUES (%(TradeID)s, %(CurrentTradeID)s, %(SK_CustomerID)s, %(SK_AccountID)s, %(SK_SecurityID)s, %(SK_CompanyID)s, %(SK_DateID)s, %(SK_TimeID)s, %(CurrentPrice)s, %(CurrentHolding)s, %(BatchID)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #             'TradeID': row.TradeID,
    #             'CurrentTradeID': row.CurrentTradeID,
    #             'SK_CustomerID': row.SK_CustomerID,
    #             'SK_AccountID': row.SK_AccountID,
    #             'SK_SecurityID': row.SK_SecurityID,
    #             'SK_CompanyID': row.SK_CompanyID,
    #             'SK_DateID': row.SK_DateID,
    #             'SK_TimeID': row.SK_TimeID,
    #             'CurrentPrice': row.CurrentPrice,
    #             'CurrentHolding': row.CurrentHolding,
    #             'BatchID': row.BatchID
    #         })

    # conn.commit()
    print("Record inserted successfully into DimFactHoldings table")
def load(rows: list[FactHoldings]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_fact_holding(row)
            print("Record inserted successfully into DimFactCashBalances table")

        elif(row.CDC_FLAG == 'U'):
            #update operation
            # Update statement for the record with the matching C_ID with field IsCurrent = True, set the EndDate field to the effective date of the update, IsCurrent = False, and IsActive = False
            #insert statement for the new record with the updated values and IsCurrent = True, set the EndDate field to December 31, 9999

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