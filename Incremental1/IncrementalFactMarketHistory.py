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
from datetime import datetime, timedelta
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

class FactMarketHistory(NamedTuple):
    CDC_FLAG: str  
    SK_SecurityID: int
    SK_CompanyID: int
    SK_DateID: int
    PERatio: float
    Yield: float
    FiftyTwoWeekHigh: float
    SK_FiftyTwoWeekHighDate: int
    FiftyTwoWeekLow: float
    SK_FiftyTwoWeekLowDate: int
    ClosePrice: float
    DayHigh: float
    DayLow: float
    Volume: int
    BatchID: int

class FactMarketHistoryIncremental(NamedTuple):
    CDC_FLAG: str
    CDC_DSN: int
    DM_DATE: date
    DM_S_SYMB: str
    DM_CLOSE: float
    DM_HIGH: float
    DM_LOW: float
    DM_VOL: int
    


def parse_daily_market_incremental(line: str) -> FactMarketHistoryIncremental:
    values = line.strip().split('|')
    
    return FactMarketHistoryIncremental(
        CDC_FLAG=values[0],
        CDC_DSN= int(values[1]) if values[1] else None,
        DM_DATE=values[2],
        DM_S_SYMB=values[3],
        DM_CLOSE= float(values[4]) if values[4] else None,
        DM_HIGH= float(values[5]) if values[5] else None,
        DM_LOW= float(values[6]) if values[6] else None,
        DM_VOL= int(values[7]) if values[7] else None,

    )

def extract() -> list[FactMarketHistoryIncremental]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch2/DailyMarket.txt',
    )
    cust_txt_string = response['Body'].read().decode('utf-8')
    #print(repr(csv_string))

    #read from customer.txt file
    # cust_txt_string = ""
    # with open('/home/reitnos/Desktop/ULB/DataWarehouses/TPCDI/staging/10/Batch2/DailyMarket2.txt', 'r') as f:
    #     cust_txt_string = f.read()

    lines = cust_txt_string.splitlines()
    #print file content
    #print(csv_string)

    accountInstanceList = []
    for line in lines:

        stripped_line = line.strip()
        accountInstance = parse_daily_market_incremental(stripped_line)
        accountInstanceList.append(accountInstance)
   
    return accountInstanceList
    
   
def transform(raw_rows):



    
    
    


    factMarketHistories = []



    for row in raw_rows:
        
        sql_for_sk_security = f"SELECT SK_SecurityID, SK_CompanyID FROM public.DimSecurity WHERE symbol = '{row.DM_S_SYMB}' and IsCurrent = True;"
        #sql_for_sk_security = sql.SQL("SELECT SK_SecurityID, SK_CompanyID FROM public.DimSecurity WHERE symbol = %s and IsCurrent = True;")
        

        #cur.execute(sql_for_sk_security, (row.DM_S_SYMB,))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_security)
        
        # Fetch all the rows
        #queryresults = cur.fetchone()
        queryresults = client_redshift.fetchone()
        sk_security = None
        sk_company = None
        if queryresults:
            sk_security = queryresults[0]
            sk_company = queryresults[1]
        
        sql_for_sk_date = f"SELECT SK_DateID FROM public.DimDate WHERE DateValue= '{row.DM_DATE}' ;"
        # sql_for_sk_date = sql.SQL("SELECT SK_DateID FROM public.DimDate WHERE DateValue= %s ;")

        # cur.execute(sql_for_sk_date, (row.DM_DATE,))
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_date)
        
        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()
        
        
        sk_date = None
        if queryresults:
            sk_date = queryresults[0]


        # FiftyTwoWeekHigh and SK_FiftyTwoWeekHighDate are determined by finding the highest
        # price over the last year (approximately 52 weeks) for a given security. The
        # FactMarketHistory table itself can be used for this comparison. FiftyTwoWeekHigh is set
        # to the highest DM_HIGH value for any date in the range from DM_DATE back to but not
        # including the same date one year earlier. SK_FiftyTwoWeekHighDate is assigned the
        # earliest date in the date range upon which this DM_HIGH value occurred.

      

        #date one year earlier
        date_one_year_earlier = datetime.strptime(row.DM_DATE, '%Y-%m-%d').date() - timedelta(days=365)
        #SK_DateID for date one year earlier
        sql_for_sk_date_one_year_earlier = f"SELECT SK_DateID FROM public.DimDate WHERE DateValue= '{date_one_year_earlier}' ;"
        #sql_for_sk_date_one_year_earlier = sql.SQL("SELECT SK_DateID FROM public.DimDate WHERE DateValue= %s ;")

        #cur.execute(sql_for_sk_date_one_year_earlier, (date_one_year_earlier,))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_date_one_year_earlier)
        
        # Fetch all the rows

        #queryresults = cur.fetchone()

        queryresults = client_redshift.fetchone()
        sk_date_one_year_earlier = None
        if queryresults:
            sk_date_one_year_earlier = queryresults[0]
        
        #SK_DateID for date of current row
        sql_for_sk_date_current_row = f"SELECT SK_DateID FROM public.DimDate WHERE DateValue= '{row.DM_DATE}' ;"
        #sql_for_sk_date_current_row = sql.SQL("SELECT SK_DateID FROM public.DimDate WHERE DateValue= %s ;")

        #cur.execute(sql_for_sk_date_current_row, (row.DM_DATE,))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_sk_date_current_row)
        
        # Fetch all the rows

        #queryresults = cur.fetchone()
        queryresults = client_redshift.fetchone()
        sk_date_current_row = None

        if queryresults:
            sk_date_current_row = queryresults[0]
        print(sk_date_current_row)

        sql_for_fifty_two_week_high = f"SELECT MAX(DAYHIGH), MIN(SK_DateID) FROM public.FactMarketHistory WHERE SK_SecurityID = {sk_security} AND SK_DateID > {sk_date_one_year_earlier} AND SK_DateID <= {sk_date_current_row};"

        #sql_for_fifty_two_week_high = sql.SQL("SELECT MAX(DAYHIGH), MIN(SK_DateID) FROM public.FactMarketHistory WHERE SK_SecurityID = %s AND SK_DateID > %s AND SK_DateID <= %s;")
        
        #cur.execute(sql_for_fifty_two_week_high, (sk_security, sk_date_one_year_earlier, sk_date_current_row))
       # cur.execute(sql_for_fifty_two_week_high, (sk_security, sk_date, sk_date))
        
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_fifty_two_week_high)
        
        queryresults = client_redshift.fetchone()

        #queryresults = cur.fetchone()

        fifty_two_week_high = None
        sk_fifty_two_week_high_date = None
        if queryresults:
            fifty_two_week_high = queryresults[0]
            sk_fifty_two_week_high_date = queryresults[1]
        print(fifty_two_week_high)
        print(sk_fifty_two_week_high_date)

        if fifty_two_week_high < row.DM_HIGH:
            fifty_two_week_high = row.DM_HIGH
            sk_fifty_two_week_high_date = sk_date_current_row
        
        # FiftyTwoWeekLow and SK_FiftyTwoWeekLowDate are determined by finding the lowest price
        # over the last year (approximately 52 weeks) for a given security. The
        # FactMarketHistory table itself can be used for this comparison. FiftyTwoWeekLow is set
        # to the lowest DM_LOW value for any date in the range from DM_DATE back to but not
        # including the same date one year earlier. SK_FiftyTwoWeekLowDate is assigned the earliest
        # date in the date range upon which this DM_LOW value occurred.
        
        sql_for_fifty_two_week_low = f"SELECT MIN(DAYLOW), MIN(SK_DateID) FROM public.FactMarketHistory WHERE SK_SecurityID = {sk_security} AND SK_DateID > {sk_date_one_year_earlier} AND SK_DateID <= {sk_date_current_row};"
        # sql_for_fifty_two_week_low = sql.SQL("SELECT MIN(DAYLOW), MIN(SK_DateID) FROM public.FactMarketHistory WHERE SK_SecurityID = %s AND SK_DateID > %s AND SK_DateID <= %s;")

        # cur.execute(sql_for_fifty_two_week_low, (sk_security, sk_date_one_year_earlier, sk_date_current_row))
        #cur.execute(sql_for_fifty_two_week_low, (sk_security, sk_date, sk_date))
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_fifty_two_week_low)

        queryresults = client_redshift.fetchone()

        #queryresults = cur.fetchone()

        fifty_two_week_low = None
        sk_fifty_two_week_low_date = None
        if queryresults:
            fifty_two_week_low = queryresults[0]
            sk_fifty_two_week_low_date = queryresults[1]
        print(fifty_two_week_low)
        print(sk_fifty_two_week_low_date)

        if fifty_two_week_low > row.DM_LOW:
            fifty_two_week_low = row.DM_LOW
            sk_fifty_two_week_low_date = sk_date_current_row

        
        # PERatio is calculated by dividing DM_CLOSE (the closing price for a security on a given
        # day) by the sum of the company’s quarterly earnings per share (“eps”) over the previous
        # 4 quarters prior to DM_DATE. Company quarterly earnings per share data was provided
        # by the FINWIRE data source in the EPS field of the ‘FIN’ record type in the Historical Load
        # phase data, and should exist in the data warehouse FINANCIAL table as a result of the
        # Historical Load transformation desribed in 4.5.14. If there are no earnings for this
        # company, NULL is assigned to PERatio and an alert condition is raised as described below.
        
        sql_for_eps = f"SELECT SUM(fi_basic_eps) FROM public.Financial WHERE SK_CompanyID = {sk_company} AND fi_qtr_start_date > {date_one_year_earlier} AND fi_qtr_start_date <= {row.DM_DATE};"
        # sql_for_eps = sql.SQL("SELECT SUM(fi_basic_eps) FROM public.Financial WHERE SK_CompanyID = %s AND fi_qtr_start_date > %s AND fi_qtr_start_date <= %s;")

        # cur.execute(sql_for_eps, (str(sk_company), date_one_year_earlier, row.DM_DATE))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_eps)
        
        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()

        eps = None
        if queryresults:
            eps = queryresults[0]
        print(eps)


        pe_ratio = None
        if eps:
            pe_ratio = row.DM_CLOSE / float(eps)
        print(pe_ratio)

        # Yield is calculated by dividing the security’s dividend by DM_CLOSE (the closing price for a
        # security on a given day), then multiplying by 100 to obtain the percentage. The dividend
        # is obtained from DimSecurity by matching DM_S_SYMB with Symbol, where IsCurrent = 1,
        # to return the Dividend field

        sql_for_dividend = f"SELECT Dividend FROM public.DimSecurity WHERE Symbol = '{row.DM_S_SYMB}' AND IsCurrent = True;"
        # sql_for_dividend = sql.SQL("SELECT Dividend FROM public.DimSecurity WHERE Symbol = %s AND IsCurrent = True;")

        # cur.execute(sql_for_dividend, (row.DM_S_SYMB,))

        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_dividend)
        
        queryresults = client_redshift.fetchone()
        #queryresults = cur.fetchone()

        dividend = None
        if queryresults:
            dividend = queryresults[0]
        print(dividend)

        yield_value = None
        if dividend:
            yield_value = (float(dividend) / row.DM_CLOSE) * 100
        print(yield_value)

        

        factMarketHistory = FactMarketHistory(

            CDC_FLAG = row.CDC_FLAG,
            SK_SecurityID = sk_security,
            SK_CompanyID = sk_company,
            SK_DateID = sk_date,
            PERatio = pe_ratio,
            Yield = yield_value,
            FiftyTwoWeekHigh = fifty_two_week_high,
            SK_FiftyTwoWeekHighDate = sk_fifty_two_week_high_date,
            FiftyTwoWeekLow = fifty_two_week_low,
            SK_FiftyTwoWeekLowDate = sk_fifty_two_week_low_date,
            ClosePrice = row.DM_CLOSE,
            DayHigh = row.DM_HIGH,
            DayLow = row.DM_LOW,
            Volume = row.DM_VOL,
            BatchID = 2
        )

        factMarketHistories.append(factMarketHistory)


            
    
    return factMarketHistories  

# def batched(iterable, chunk_size):
#     iterator = iter(iterable)
#     while chunk := tuple(islice(iterator, chunk_size)):
#         yield chunk
def insert_fact_market_history(row: FactMarketHistory):
    sql_for_insert = f"""
        INSERT INTO public.FactMarketHistory (SK_SecurityID, SK_CompanyID, SK_DateID, PERatio, Yield, FiftyTwoWeekHigh, SK_FiftyTwoWeekHighDate, FiftyTwoWeekLow, SK_FiftyTwoWeekLowDate, ClosePrice, DayHigh, DayLow, Volume, BatchID)
        VALUES ({row.SK_SecurityID}, {row.SK_CompanyID}, {row.SK_DateID}, {row.PERatio}, {row.Yield}, {row.FiftyTwoWeekHigh}, {row.SK_FiftyTwoWeekHighDate}, {row.FiftyTwoWeekLow}, {row.SK_FiftyTwoWeekLowDate}, {row.ClosePrice}, {row.DayHigh}, {row.DayLow}, {row.Volume}, {row.BatchID});
    """

    client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=sql_for_insert)
    
    
    # sql_for_insert = '''
    #     INSERT INTO FactMarketHistory (SK_SecurityID, SK_CompanyID, SK_DateID, PERatio, Yield, FiftyTwoWeekHigh, SK_FiftyTwoWeekHighDate, FiftyTwoWeekLow, SK_FiftyTwoWeekLowDate, ClosePrice, DayHigh, DayLow, Volume, BatchID)
    #     VALUES (%(SK_SecurityID)s, %(SK_CompanyID)s, %(SK_DateID)s, %(PERatio)s, %(Yield)s, %(FiftyTwoWeekHigh)s, %(SK_FiftyTwoWeekHighDate)s, %(FiftyTwoWeekLow)s, %(SK_FiftyTwoWeekLowDate)s, %(ClosePrice)s, %(DayHigh)s, %(DayLow)s, %(Volume)s, %(BatchID)s);
    #     '''
    # cur.execute(sql_for_insert, {
    #             'SK_SecurityID': row.SK_SecurityID,
    #             'SK_CompanyID': row.SK_CompanyID,
    #             'SK_DateID': row.SK_DateID,
    #             'PERatio': row.PERatio,
    #             'Yield': row.Yield,
    #             'FiftyTwoWeekHigh': row.FiftyTwoWeekHigh,
    #             'SK_FiftyTwoWeekHighDate': row.SK_FiftyTwoWeekHighDate,
    #             'FiftyTwoWeekLow': row.FiftyTwoWeekLow,
    #             'SK_FiftyTwoWeekLowDate': row.SK_FiftyTwoWeekLowDate,
    #             'ClosePrice': row.ClosePrice,
    #             'DayHigh': row.DayHigh,
    #             'DayLow': row.DayLow,
    #             'Volume': row.Volume,
    #             'BatchID': row.BatchID
    #         })

    # conn.commit()
    print("Record inserted successfully into DimMarketHistory table")
def load(rows: list[FactMarketHistory]):

    for row in rows:


        if(row.CDC_FLAG == 'I'):

            insert_fact_market_history(row)
            print("Record inserted successfully into DimFactMarketHistory table")

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
    
    # # for row in rows:
    # #     print(row)

    response = load(rows)
        
    return str(response)


lambda_handler(0,0)