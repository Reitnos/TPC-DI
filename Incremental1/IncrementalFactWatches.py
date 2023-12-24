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
#DONE#
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

# Establish a connection to the PostgreSQL database
##conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

#print the databases
##cur = conn.cursor()

#class Account(NamedTuple):

class FactWatches(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    SK_CustomerID: int
    SK_SecurityID: int
    SK_DateID_DatePlaced: int
    SK_DateID_DateRemoved: int
    BatchID: int

class FactWatchesIncremental(NamedTuple):
    CDC_FLAG: str
    CDC_DSN: int
    W_C_ID: str
    W_S_SYMB: str
    W_DTS: datetime
    W_ACTION: str 
 


def parse_watch_history_incremental(line: str) -> FactWatchesIncremental:
    values = line.strip().split('|')
    
    return FactWatchesIncremental(
        CDC_FLAG=values[0],
        CDC_DSN= int(values[1]) if values[1] else None,
        W_C_ID=values[2],
        W_S_SYMB=values[3],
        W_DTS=values[4],
        W_ACTION=values[5]


    )

def extract() -> list[FactWatchesIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/WatchHistory.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    # print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/WatchHistory2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_watch_history_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):


   

    
    
    
    


    factWatchesList = []



    for row in raw_rows:

        # SK_CustomerID – each watch list is associated with a customer. W_C_ID can be used to
        # match the associated DimCustomer record, W_C_ID = C_ID where IsCurrent=1, to obtain
        # SK_CustomerID
        sql_for_sk_customer_id = f"SELECT SK_CustomerID FROM DimCustomer WHERE customerid = {row.W_C_ID} AND IsCurrent = True"
        #sql_for_sk_customer_id = sql.SQL(" SELECT SK_CustomerID FROM DimCustomer WHERE customerid = %s AND IsCurrent = True ")

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_customer_id)  
        #cur.execute(sql_for_sk_customer_id, (row.W_C_ID,))

        #res = cur.fetchone()
        res = client_redshift.fetchone()
        SK_CustomerID = None    
        if res:
            SK_CustomerID = res[0]
        print(SK_CustomerID)

        # SK_SecurityID – W_S_SYMB can be used to match the current associated DimSecurity
        # record, W_SYMB = Symbol where IsCurrent=1, to obtain SK_ SecurityID.
        sql_for_sk_security_id = f"SELECT SK_SecurityID FROM DimSecurity WHERE Symbol = '{row.W_S_SYMB}' AND IsCurrent = True"
        #sql_for_sk_security_id = sql.SQL(" SELECT SK_SecurityID FROM DimSecurity WHERE Symbol = %s AND IsCurrent = True ")

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_security_id)
        

        #cur.execute(sql_for_sk_security_id, (row.W_S_SYMB,))
        #res = cur.fetchone()
        res = client_redshift.fetchone()
        SK_SecurityID = None
        if res:
            SK_SecurityID = res[0]

    
        
        #W_ACTION = “ACTV”,

        #BatchID – set to 2.
        BatchID = 2
        if(row.W_ACTION == 'ACTV'):
       

            #SK_DateID_DatePlaced – set based on W_DTS.
            sql_for_sk_date_id_date_placed = f"SELECT SK_DateID FROM DimDate WHERE datevalue = '{row.W_DTS}'"
            #sql_for_sk_date_id_date_placed = sql.SQL(" SELECT SK_DateID FROM DimDate WHERE datevalue = %s ")

            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_date_id_date_placed)
            

            #cur.execute(sql_for_sk_date_id_date_placed, (row.W_DTS,))

            SK_DateID_DatePlaced = None
            #res = cur.fetchone()
            res = client_redshift.fetchone()
            if res:
                SK_DateID_DatePlaced = res[0]



            #SK_DateID_DateRemoved – set to NULL.
            SK_DateID_DateRemoved = None

            factWatches = FactWatches(
                CDC_FLAG= 'I',
                SK_CustomerID = SK_CustomerID,
                SK_SecurityID = SK_SecurityID,
                SK_DateID_DatePlaced = SK_DateID_DatePlaced,
                SK_DateID_DateRemoved = SK_DateID_DateRemoved,
                BatchID = BatchID
            
             )
        
            factWatchesList.append(factWatches)


        elif row.W_ACTION == 'CNCL':
            #SK_DateID_DateRemoved – set based on W_DTS.

            sql_for_sk_date_id_date_removed = f"SELECT SK_DateID FROM DimDate WHERE datevalue = '{row.W_DTS}'"
            #sql_for_sk_date_id_date_removed = sql.SQL(" SELECT SK_DateID FROM DimDate WHERE datevalue = %s ")

            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_date_id_date_removed)
            
            #cur.execute(sql_for_sk_date_id_date_removed, (row.W_DTS,))

            #res = cur.fetchone()
            res = client_redshift.fetchone()
            SK_DateID_DateRemoved = None
            if res:
                SK_DateID_DateRemoved = res[0]
            

            #BatchID – set to 2.
            factWatches = FactWatches(
                CDC_FLAG= 'U',
                SK_CustomerID = SK_CustomerID,
                SK_SecurityID = SK_SecurityID,
                SK_DateID_DatePlaced = None,
                SK_DateID_DateRemoved = SK_DateID_DateRemoved,
                BatchID = BatchID
            )
            
            factWatchesList.append(factWatches)
        
        

        

        
        
            
    
    return factWatchesList

# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_fact_watches(row: FactWatches):

    sql_for_insert = f"""
        INSERT INTO FactWatches (SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced, SK_DateID_DateRemoved, BatchID)
        VALUES ({row.SK_CustomerID}, {row.SK_SecurityID}, {row.SK_DateID_DatePlaced}, {row.SK_DateID_DateRemoved}, {row.BatchID});
        """

    client_redshift.execute_statement(
        Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    
    

    # sql_for_insert = '''
    #     INSERT INTO FactWatches (SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced, SK_DateID_DateRemoved, BatchID)
    #     VALUES (%(SK_CustomerID)s, %(SK_SecurityID)s, %(SK_DateID_DatePlaced)s, %(SK_DateID_DateRemoved)s, %(BatchID)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #             'SK_CustomerID': row.SK_CustomerID,
    #             'SK_SecurityID': row.SK_SecurityID,
    #             'SK_DateID_DatePlaced': row.SK_DateID_DatePlaced,
    #             'SK_DateID_DateRemoved': row.SK_DateID_DateRemoved,
    #             'BatchID': row.BatchID
    #         })

    # conn.commit()
    print("Record inserted successfully into FactWatches table")
def load(rows: list[FactWatches]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_fact_watches(row)
            

        elif(row.CDC_FLAG == 'U'):
            
            # update the existing record in the table

            sql_for_update = f'''
                UPDATE FactWatches
                SET SK_DateID_DateRemoved = {row.SK_DateID_DateRemoved}
                WHERE SK_CustomerID = {row.SK_CustomerID} AND SK_SecurityID = {row.SK_SecurityID};
            '''

            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            
            # sql_for_update = '''
            #     UPDATE FactWatches
            #     SET SK_DateID_DateRemoved = %(SK_DateID_DateRemoved)s
            #     WHERE SK_CustomerID = %(SK_CustomerID)s AND SK_SecurityID = %(SK_SecurityID)s;
            # '''

            # cur.execute(sql_for_update, {
            #     'SK_DateID_DateRemoved': row.SK_DateID_DateRemoved,
            #     'SK_CustomerID': row.SK_CustomerID,
            #     'SK_SecurityID': row.SK_SecurityID,
            # })
            

            # conn.commit()
        
            print("Record updated successfully into FactWatches table")
            
    return 0
        

            
  
        
    

def lambda_handler(event, context):
    raw_rows = extract()
    rows = transform(raw_rows)
    
    # for row in rows:
    #     print(row)

    response = load(rows)
        
    return str(response)
    #return 0

lambda_handler(0,0)