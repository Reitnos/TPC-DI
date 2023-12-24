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
from datetime import time

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

class DimTrade(NamedTuple):
    CDC_FLAG: str  # 'I' or 'U' Denotes insert or update
    TradeID: str
    SK_BrokerID: int
    SK_CreateDateID: int
    SK_CreateTimeID: int
    SK_CloseDateID: int
    SK_CloseTimeID: int
    Status: str
    Type: str
    CashFlag: bool
    SK_SecurityID: int
    SK_CompanyID: int
    Quantity: int
    BidPrice: float
    SK_CustomerID: int
    SK_AccountID: int
    ExecutedBy: str
    TradePrice: float
    Fee: float
    Commission: float
    Tax: float
    BatchID: int

class DimTradeIncremental(NamedTuple):
   
    CDC_FLAG: str
    CDC_DSN: int
    T_ID: str
    T_DTS: datetime
    T_ST_ID: str
    T_TT_ID: str
    T_IS_CASH: bool
    T_S_SYMB: str
    T_QTY: int
    T_BID_PRICE: float
    T_CA_ID: str
    T_EXEC_NAME: str
    T_TRADE_PRICE: float
    T_CHRG: float
    T_COMM: float
    T_TAX: float


def parse_trade_incremental(line: str) -> DimTradeIncremental:
    values = line.strip().split('|')
    
    return DimTradeIncremental(
        
        CDC_FLAG=values[0],
        CDC_DSN= int(values[1]) if values[1] else None,
        T_ID= values[2],
        T_DTS= values[3],
        T_ST_ID= values[4],
        T_TT_ID= values[5],
        T_IS_CASH= bool(values[6]) if values[6] else None,
        T_S_SYMB= values[7],
        T_QTY= int(values[8]) if values[8] else None,
        T_BID_PRICE= float(values[9]) if values[9] else None,
        T_CA_ID= values[10],
        T_EXEC_NAME= values[11],
        T_TRADE_PRICE= float(values[12]) if values[12] else None,
        T_CHRG= float(values[13]) if values[13] else None,
        T_COMM= float(values[14]) if values[14] else None,
        T_TAX= float(values[15]) if values[15] else None,



    )

def extract() -> list[DimTradeIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/Trade.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/Trade2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_trade_incremental(stripped_line)
        accountInstanceList.append(accountInstance)

   
    return accountInstanceList
def transform(raw_rows):

    


    dimTrades = []


    for row in raw_rows:

        
        # If this is a new Trade record (CDC_FLAG = “I”) then SK_CreateDateID and
        # SK_CreateTimeID must be set based on T_DTS. SK_CloseDateID and SK_CloseTimeID
        # must be set to NULL.

        sql_for_sk_create_date_id = f" SELECT SK_DateID FROM DimDate WHERE DateValue = '{row.T_DTS}' "


        #sql_for_sk_create_date_id = sql.SQL(" SELECT SK_DateID FROM DimDate WHERE DateValue = %s ")
        #T_DTS is in string format. We need to convert it to datetime format
        #cur.execute(sql_for_sk_create_date_id, (row.T_DTS,))
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_create_date_id)
        queryresults = client_redshift.fetchone()
        
        
        SK_CreateDateID = None
        if queryresults:
            SK_CreateDateID = queryresults[0]
        print(SK_CreateDateID)
        
        sql_for_sk_create_time_id= f" SELECT SK_TimeID FROM DimTime WHERE TimeValue = '{row.T_DTS}' "

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_create_time_id)
        queryresults = client_redshift.fetchone()

        #sql_for_sk_createtimeid= sql.SQL(" SELECT SK_TimeID FROM DimTime WHERE TimeValue = %s ")

        #cur.execute(sql_for_sk_createtimeid, (row.T_DTS,))
       # queryresults = cur.fetchone()
        SK_CreateTimeID = None
        if queryresults:
            SK_CreateTimeID = queryresults[0]
        print(SK_CreateTimeID)


        SK_CloseDateID = None
        SK_CloseTimeID = None
        if(row.T_ST_ID == 'CMPT' or row.T_ST_ID == 'CNCL'):
            SK_CloseDateID = SK_CreateDateID
            SK_CloseTimeID = SK_CreateTimeID
        
        #STATUS
            
        sql_for_sk_status_name = f" SELECT st_name FROM statustype WHERE st_id = '{row.T_ST_ID}' "

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_status_name)
        queryresults = client_redshift.fetchone()

        #sql_for_sk_status_name = sql.SQL(" SELECT st_name FROM statustype WHERE st_id = %s ")

        #cur.execute(sql_for_sk_status_name, (row.T_ST_ID,))
        #queryresults = cur.fetchone()
        status = None
        if queryresults:
            status = queryresults[0]
        print(status)

        #Type is copied from TT_NAME of the TradeType table by matching T_TT_ID with TT_ID.
        sql_for_sk_type_name = f" SELECT tt_name FROM tradetype WHERE tt_id = '{row.T_TT_ID}' "

        #sql_for_sk_type_name = sql.SQL(" SELECT tt_name FROM tradetype WHERE tt_id = %s ")

        #cur.execute(sql_for_sk_type_name, (row.T_TT_ID,))
        #queryresults = cur.fetchone()
       
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_type_name)
        queryresults = client_redshift.fetchone()

       
        type = None
        if queryresults:
            type = queryresults[0]
        print(type)
        
        #SK_SecurityID and SK_CompanyID are copied from SK_SecurityID and SK_CompanyID of the DimSecurity table by matching T_S_SYMB with Symbol where IsCurrent = 1
        
        sql_for_sk_security_and_company_id = f" SELECT SK_SecurityID, SK_CompanyID FROM DimSecurity WHERE Symbol = '{row.T_S_SYMB}' AND IsCurrent = True "

        #sql_for_sk_security_and_company_id = sql.SQL(" SELECT SK_SecurityID, SK_CompanyID FROM DimSecurity WHERE Symbol = %s AND IsCurrent = True ")

        #cur.execute(sql_for_sk_security_and_company_id, (row.T_S_SYMB,))
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_security_and_company_id)
        queryresults = client_redshift.fetchone()

        
        SK_SecurityID = None
        SK_CompanyID = None
        if queryresults:
            SK_SecurityID = queryresults[0]
            SK_CompanyID = queryresults[1]
        print(SK_SecurityID)
        print(SK_CompanyID)

        #SK_AccountID, SK_CustomerID, and SK_BrokerID are copied from the SK_AccountID, SK_CustomerID, and SK_BrokerID fields of the DimAccount table by matching T_CA_ID with AccountID where IsCurrent = 1. 

        sql_for_sk_account_customer_broker_id = f" SELECT SK_AccountID, SK_CustomerID, SK_BrokerID FROM DimAccount WHERE AccountID = '{row.T_CA_ID}' AND IsCurrent = True "

        #sql_for_sk_account_customer_broker_id = sql.SQL(" SELECT SK_AccountID, SK_CustomerID, SK_BrokerID FROM DimAccount WHERE AccountID = %s AND IsCurrent = True ")

        #cur.execute(sql_for_sk_account_customer_broker_id, (row.T_CA_ID,))
        #queryresults = cur.fetchone()
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_account_customer_broker_id)
        queryresults = client_redshift.fetchone()

        
        SK_AccountID = None
        SK_CustomerID = None
        SK_BrokerID = None
        if queryresults:
            SK_AccountID = queryresults[0]
            SK_CustomerID = queryresults[1]
            SK_BrokerID = queryresults[2]
        print(SK_AccountID)
        print(SK_CustomerID)
        print(SK_BrokerID)

        batchID = 2
       
           
        dimTrade = DimTrade(
            CDC_FLAG=row.CDC_FLAG,
            TradeID=row.T_ID,
            SK_BrokerID=SK_BrokerID,
            SK_CreateDateID=SK_CreateDateID,
            SK_CreateTimeID=SK_CreateTimeID,
            SK_CloseDateID=SK_CloseDateID,
            SK_CloseTimeID=SK_CloseTimeID,
            Status=status,
            Type=type,
            CashFlag=row.T_IS_CASH,
            SK_SecurityID=SK_SecurityID,
            SK_CompanyID=SK_CompanyID,
            Quantity=row.T_QTY,
            BidPrice=row.T_BID_PRICE,
            SK_CustomerID=SK_CustomerID,
            SK_AccountID=SK_AccountID,
            ExecutedBy=row.T_EXEC_NAME,
            TradePrice=row.T_TRADE_PRICE,
            Fee=row.T_CHRG,
            Commission=row.T_COMM,
            Tax=row.T_TAX,
            BatchID=batchID

        )
        dimTrades.append(dimTrade)
       

        # dimTrade = DimTrade(
        # )
        # dimTrades.append(dimTrade)
    
    return dimTrades

# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_dim_trade(row: DimTrade):

    sql_for_insert = f"""
        INSERT INTO DimTrade (
            TradeID,
            SK_BrokerID,
            SK_CreateDateID,
            SK_CreateTimeID,
            SK_CloseDateID,
            SK_CloseTimeID,
            Status,
            Type,
            CashFlag,
            SK_SecurityID,
            SK_CompanyID,
            Quantity,
            BidPrice,
            SK_CustomerID,
            SK_AccountID,
            ExecutedBy,
            TradePrice,
            Fee,
            Commission,
            Tax,
            BatchID
        )
        VALUES (
            {row.TradeID},
            {row.SK_BrokerID},
            {row.SK_CreateDateID},
            {row.SK_CreateTimeID},
            {row.SK_CloseDateID},
            {row.SK_CloseTimeID},
            {row.Status},
            {row.Type},
            {row.CashFlag},
            {row.SK_SecurityID},
            {row.SK_CompanyID},
            {row.Quantity},
            {row.BidPrice},
            {row.SK_CustomerID},
            {row.SK_AccountID},
            {row.ExecutedBy},
            {row.TradePrice},
            {row.Fee},
            {row.Commission},
            {row.Tax},
            {row.BatchID}
        );
        """

    
    client_redshift.execute_statement(
        Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    
    # cur.execute(sql_for_insert, {
    #             'TradeID': row.TradeID,
    #             'SK_BrokerID': row.SK_BrokerID,
    #             'SK_CreateDateID': row.SK_CreateDateID,
    #             'SK_CreateTimeID': row.SK_CreateTimeID,
    #             'SK_CloseDateID': row.SK_CloseDateID,
    #             'SK_CloseTimeID': row.SK_CloseTimeID,
    #             'Status': row.Status,
    #             'Type': row.Type,
    #             'CashFlag': row.CashFlag,
    #             'SK_SecurityID': row.SK_SecurityID,
    #             'SK_CompanyID': row.SK_CompanyID,
    #             'Quantity': row.Quantity,
    #             'BidPrice': row.BidPrice,
    #             'SK_CustomerID': row.SK_CustomerID,
    #             'SK_AccountID': row.SK_AccountID,
    #             'ExecutedBy': row.ExecutedBy,
    #             'TradePrice': row.TradePrice,
    #             'Fee': row.Fee,
    #             'Commission': row.Commission,
    #             'Tax': row.Tax,
    #             'BatchID': row.BatchID

    #         })

    # conn.commit()
    print("Record inserted successfully into DimTrade table")
def load(rows: list[DimTrade]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_dim_trade(row)
            

        elif(row.CDC_FLAG == 'U'):
            
            # update the existing record in the table
            sql_for_update = f'''
                UPDATE DimTrade
                SET
                    SK_CreateDateID = {row.SK_CreateDateID},
                    SK_CreateTimeID = {row.SK_CreateTimeID},
                    SK_CloseDateID = {row.SK_CloseDateID},
                    SK_CloseTimeID = {row.SK_CloseTimeID},
                    Status = {row.Status},
                    Type = {row.Type},
                    SK_SecurityID = {row.SK_SecurityID},
                    SK_CompanyID = {row.SK_CompanyID},
                    SK_AccountID = {row.SK_AccountID},
                    SK_CustomerID = {row.SK_CustomerID},
                    SK_BrokerID = {row.SK_BrokerID},
                    IsCash = {row.CashFlag},
                    Quantity = {row.Quantity},
                    BidPrice = {row.BidPrice},
                    TradePrice = {row.TradePrice},
                    Fee = {row.Fee},
                    Commission = {row.Commission},
                    Tax = {row.Tax},
                    BatchID = {row.BatchID}
                WHERE
                    tradeid = {row.TradeID}
            '''
            client_redshift.execute_statement(
                Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_update)
            # sql_for_update = '''
            #     UPDATE DimTrade
            #     SET
            #         SK_CreateDateID = %(SK_CreateDateID)s,
            #         SK_CreateTimeID = %(SK_CreateTimeID)s,
            #         SK_CloseDateID = %(SK_CloseDateID)s,
            #         SK_CloseTimeID = %(SK_CloseTimeID)s,
            #         SK_StatusID = %(SK_StatusID)s,
            #         Status = %(Status)s,
            #         SK_TypeID = %(SK_TypeID)s,
            #         Type = %(Type)s,
            #         SK_SecurityID = %(SK_SecurityID)s,
            #         SK_CompanyID = %(SK_CompanyID)s,
            #         SK_AccountID = %(SK_AccountID)s,
            #         SK_CustomerID = %(SK_CustomerID)s,
            #         SK_BrokerID = %(SK_BrokerID)s,
            #         IsCash = %(IsCash)s,
            #         Quantity = %(Quantity)s,
            #         BidPrice = %(BidPrice)s,
            #         TradePrice = %(TradePrice)s,
            #         Charge = %(Charge)s,
            #         Commission = %(Commission)s,
            #         Tax = %(Tax)s,
            #         CDC_FLAG = %(CDC_FLAG)s,
            #         CDC_DSN = %(CDC_DSN)s,
            #         BatchID = %(BatchID)s
            #     WHERE
            #         tradeid = %(T_ID)s
            # '''
            #conn.commit()

            print("Record updated successfully into DimTrade table")
            
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