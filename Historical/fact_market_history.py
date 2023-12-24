from typing import List, NamedTuple, Optional
from datetime import datetime, timedelta


# Define the DimSecurity NamedTuple
class DimSecurity(NamedTuple):
    Symbol: str
    SK_SecurityID: int
    SK_CompanyID: int
    EffectiveDate: datetime
    EndDate: datetime

# Define the FactMarketHistory NamedTuple
class FactMarketHistory(NamedTuple):
    DM_DATE: datetime
    DM_S_SYMB: str
    DM_CLOSE: float
    DM_HIGH: float
    DM_LOW: float
    DM_VOL: int

# Function to parse DailyMarket.txt and create FactMarketHistory records
def parse_daily_market_file(file_path: str) -> List[FactMarketHistory]:
    market_history = []

    # Open the file and read lines
    with open(file_path, 'r') as file:
        for line in file:
            # Split fields by vertical bar ("|")
            fields = line.strip().split('|')

            # Extract values and create FactMarketHistory record
            dm_date = datetime.strptime(fields[2], "%Y-%m-%d")
            dm_s_symb, dm_close, dm_high, dm_low, dm_vol = fields[3], float(fields[4]), float(fields[5]), float(fields[6]), int(fields[7])
            market_record = FactMarketHistory(DM_DATE=dm_date, DM_S_SYMB=dm_s_symb, DM_CLOSE=dm_close, DM_HIGH=dm_high, DM_LOW=dm_low, DM_VOL=dm_vol)
            market_history.append(market_record)

    return market_history

# Example usage
daily_market_file_path = "path/to/DailyMarket.txt"
market_history_result = parse_daily_market_file(daily_market_file_path)

# Define the FactMarketHistoryProcessed NamedTuple
class FactMarketHistoryProcessed(NamedTuple):
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

# Function to process FactMarketHistory records
def process_fact_market_history(records: List[FactMarketHistory], dim_security_records: List[DimSecurity], dim_company_records: list[DimCompany]) -> List[FactMarketHistoryProcessed]:
    processed_records = []

    for record in records:
        # Find associated DimSecurity record
        dim_security_record = next((ds for ds in dim_security_records if ds.Symbol == record.DM_S_SYMB and record.DM_DATE == ds.EffectiveDate), None)
        dim_company_record = next((dc for dc in dim_company_records if dc.Symbol == record.DM_S_SYMB and record.DM_DATE == dc.EffectiveDate), None)
        if dim_security_record and dim_company_record:
            # Common fields
            sk_security_id = dim_security_record.SK_SecurityID
            sk_company_id = dim_company_record.SK_CompanyID
            sk_date_id = 1  # Replace with actual logic, join from DateDim

            # FiftyTwoWeekHigh logic
            fifty_two_week_high = max(
                fh.DM_HIGH for fh in records
                if fh.DM_S_SYMB == record.DM_S_SYMB
                and record.DM_DATE - timedelta(days=365) <= fh.DM_DATE < record.DM_DATE
            )
            sk_fifty_two_week_high_date = min(
                fh.SK_DateID for fh in records
                if fh.DM_S_SYMB == record.DM_S_SYMB
                and record.DM_DATE - timedelta(days=365) <= fh.DM_DATE < record.DM_DATE
            )

            # FiftyTwoWeekLow logic
            fifty_two_week_low = min(
                fh.DM_LOW for fh in records
                if fh.DM_S_SYMB == record.DM_S_SYMB
                and record.DM_DATE - timedelta(days=365) <= fh.DM_DATE < record.DM_DATE
            )
            sk_fifty_two_week_low_date = min(
                fh.SK_DateID for fh in records
                if fh.DM_S_SYMB == record.DM_S_SYMB
                and record.DM_DATE - timedelta(days=365) <= fh.DM_DATE < record.DM_DATE
            )

            # PERatio and Yield logic
            sum_eps = sum(eps_values) # Replace with PERatio is calculated by dividing DM_CLOSE (the closing price for a security on a given
#                                       day) by the sum of the company’s quarterly earnings per share (“eps”) over the previous
#                                       4 quarters prior to DM_DATE. Company quarterly earnings per share data is provided by
#                                       the FINWIRE data source in the EPS field of the ‘FIN’ record type. If there are no earnings
#                                       for this company, NULL is assigned to PERatio and an alert condition is raised as described below.
            if sum_eps > 0:
                pe_ratio = record.DM_CLOSE / sum_eps
                dividend = 2.5  # Yield is calculated by dividing the security’s dividend by DM_CLOSE (the closing price for a
                                # security on a given day), then multiplying by 100 to obtain the percentage. The dividend
                                # is obtained from DimSecurity by matching DM_S_SYMB with Symbol, where DM_DATE is
                                # in the range given by EffectiveDate and EndDate, to return the Dividend field.
                yield_value = (dividend / record.DM_CLOSE) * 100
            else:
                # No earnings for this company
                pe_ratio = None
                yield_value = None

            # BatchID logic
            batch_id = 1  # Replace with actual logic

            # Create FactMarketHistoryProcessed record
            processed_record = FactMarketHistoryProcessed(
                SK_SecurityID=sk_security_id,
                SK_CompanyID=sk_company_id,
                SK_DateID=sk_date_id,
                PERatio=pe_ratio,
                Yield=yield_value,
                FiftyTwoWeekHigh=fifty_two_week_high,
                SK_FiftyTwoWeekHighDate=sk_fifty_two_week_high_date,
                FiftyTwoWeekLow=fifty_two_week_low,
                SK_FiftyTwoWeekLowDate=sk_fifty_two_week_low_date,
                ClosePrice=record.DM_CLOSE,
                DayHigh=record.DM_HIGH,
                DayLow=record.DM_LOW,
                Volume=record.DM_VOL,
                BatchID=batch_id
            )

            processed_records.append(processed_record)

    return processed_records
