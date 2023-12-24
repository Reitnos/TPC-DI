import datetime
from dataclasses import dataclass
import os
from typing import NamedTuple, Optional
import datetime

@dataclass
class BaseRecord:
    PTS: datetime.datetime
    RecType: str


@dataclass
class StatusType:
    ST_ID: str
    ST_NAME: str

@dataclass
class CMPRecord(BaseRecord):
    CompanyName: str
    CIK: str
    Status: str
    IndustryID: str
    SPrating: str
    FoundingDate: str
    AddrLine1: str
    AddrLine2: str
    PostalCode: str
    City: str
    StateProvince: str
    Country: str
    CEOname: str
    Description: str

@dataclass
class SECRecord(BaseRecord):
    Symbol: str
    IssueType: str
    Status: str
    Name: str
    ExID: str
    ShOut: str
    FirstTradeDate: str
    FirstTradeExchg: str
    Dividend: str
    CoNameOrCIK: str

@dataclass
class FINRecord(BaseRecord):
    Year: str
    Quarter: str
    QtrStartDate: str
    PostingDate: str
    Revenue: str
    Earnings: str
    EPS: str
    DilutedEPS: str
    Margin: str
    Inventory: str
    Assets: str
    Liabilities: str
    ShOut: str
    DilutedShOut: str
    CoNameOrCIK: str

def parse_cmp_record(line):
    pts = line[0:15].strip()
    rec_type = line[15:18].strip()
    company_name = line[18:78].strip()
    cik = line[78:88].strip()
    status = line[88:92].strip()
    industry_id = line[92:94].strip()
    sp_rating = line[94:98].strip()
    founding_date = line[98:106].strip()
    addr_line1 = line[106:186].strip()
    addr_line2 = line[186:266].strip()
    postal_code = line[266:278].strip()
    city = line[278:303].strip()
    state_province = line[303:323].strip()
    country = line[323:347].strip()
    ceo_name = line[347:393].strip()
    description = line[393:543].strip()

    return CMPRecord(**{
        "PTS": datetime.datetime.strptime(pts, "%Y%m%d-%H%M%S"),
        "RecType": rec_type,
        "CompanyName": company_name,
        "CIK": cik,
        "Status": status,
        "IndustryID": industry_id,
        "SPrating": sp_rating,
        "FoundingDate": founding_date,
        "AddrLine1": addr_line1,
        "AddrLine2": addr_line2,
        "PostalCode": postal_code,
        "City": city,
        "StateProvince": state_province,
        "Country": country,
        "CEOname": ceo_name,
        "Description": description
    })

def parse_sec_record(line):
    pts = line[0:15].strip()
    rec_type = line[15:18].strip()
    symbol = line[18:33].strip()
    issue_type = line[33:39].strip()
    status = line[39:43].strip()
    name = line[43:113].strip()
    ex_id = line[113:119].strip()
    sh_out = line[119:132].strip()
    first_trade_date = line[132:140].strip()
    first_trade_exchg = line[140:148].strip()
    dividend = line[148:160].strip()
    co_name_or_cik = line[160:220].strip()

    return SECRecord(**{
        "PTS": datetime.datetime.strptime(pts, "%Y%m%d-%H%M%S"),
        "RecType": rec_type,
        "Symbol": symbol,
        "IssueType": issue_type,
        "Status": status,
        "Name": name,
        "ExID": ex_id,
        "ShOut": sh_out,
        "FirstTradeDate": first_trade_date,
        "FirstTradeExchg": first_trade_exchg,
        "Dividend": dividend,
        "CoNameOrCIK": co_name_or_cik
    })

def parse_fin_record(line):
    pts = line[0:15].strip()
    rec_type = line[15:18].strip()
    year = line[18:22].strip()
    quarter = line[22].strip()
    qtr_start_date = line[23:31].strip()
    posting_date = line[31:39].strip()
    revenue = line[39:56].strip()
    earnings = line[56:73].strip()
    eps = line[73:85].strip()
    diluted_eps = line[85:97].strip()
    margin = line[97:109].strip()
    inventory = line[109:126].strip()
    assets = line[126:143].strip()
    liabilities = line[143:160].strip()
    sh_out = line[160:173].strip()
    diluted_sh_out = line[173:186].strip()
    co_name_or_cik = line[186:246].strip()

    return FINRecord(**{
        "PTS": datetime.datetime.strptime(pts, "%Y%m%d-%H%M%S"),
        "RecType": rec_type,
        "Year": year,
        "Quarter": quarter,
        "QtrStartDate": qtr_start_date,
        "PostingDate": posting_date,
        "Revenue": revenue,
        "Earnings": earnings,
        "EPS": eps,
        "DilutedEPS": diluted_eps,
        "Margin": margin,
        "Inventory": inventory,
        "Assets": assets,
        "Liabilities": liabilities,
        "ShOut": sh_out,
        "DilutedShOut": diluted_sh_out,
        "CoNameOrCIK": co_name_or_cik
    })

def parse_file(file_path) -> list[BaseRecord]:
    records = []
    for line in file:
        rec_type = line[15:18].strip()
        if rec_type == 'CMP':
            records.append(parse_cmp_record(line))
        elif rec_type == 'SEC':
            records.append(parse_sec_record(line))
        elif rec_type == 'FIN':
            records.append(parse_fin_record(line))
    return records



class DimCompany(NamedTuple):
    CompanyID: str
    Name: Optional[str]
    SPRating: Optional[str]
    CEO: Optional[str]
    Description: Optional[str]
    FoundingDate: Optional[str]
    AddressLine1: Optional[str]
    AddressLine2: Optional[str]
    PostalCode: Optional[str]
    City: Optional[str]
    State_Prov: Optional[str]
    Country: Optional[str]
    Status: Optional[str]
    Industry: Optional[str]
    isLowGrade: Optional[bool]
    IsCurrent: Optional[bool]
    EffectiveDate: Optional[datetime.datetime]
    EndDate: Optional[datetime.datetime]
    BatchID: Optional[int]

@dataclass
class DimMessage:
    MessageSource: str
    MessageType: str
    MessageText: str
    MessageData: str

def is_valid_sprating(sprating: str) -> bool:
    valid_values = {'AAA', 'AA+', 'AA-', 'A+', 'A-', 'BBB+', 'BBB-', 'BB+', 'BB-', 'B+', 'B-', 'CCC+', 'CCC-', 'CC', 'C', 'D'}
    return sprating in valid_values

def parse_dim_company_records(cmp_records: list[CMPRecord]) -> list[DimCompany]:
    dim_company_records = []
    dim_messages = []

    for cmp_record in cmp_records:
        # Extract relevant fields from CMPRecord
        company_id = cmp_record.CIK
        name = cmp_record.CompanyName.strip() or None
        sp_rating = cmp_record.SPrating.strip() or None
        ceo = cmp_record.CEOname.strip() or None
        description = cmp_record.Description.strip() or None
        founding_date = cmp_record.FoundingDate.strip() or None
        address_line1 = cmp_record.AddrLine1.strip() or None
        address_line2 = cmp_record.AddrLine2.strip() or None
        postal_code = cmp_record.PostalCode.strip() or None
        city = cmp_record.City.strip() or None
        state_prov = cmp_record.StateProvince.strip() or None
        country = cmp_record.Country.strip() or None
        status = cmp_record.Status.strip() or None
        industry = cmp_record.IndustryID.strip() or None

        # Determine isLowGrade
        is_low_grade = sp_rating is not None and not sp_rating.startswith(('A', 'BBB'))

        # Determine EffectiveDate
        effective_date = cmp_record.PTS

        is_current = True
        end_date = datetime.datetime(9999, 12, 31)
        batch_id = 1

        # Validate SPRating
        if sp_rating is not None and not is_valid_sprating(sp_rating):
            # Create DimMessage record for invalid SPRating
            message_source = "DimCompany"
            message_type = "Alert"
            message_text = "Invalid SPRating"
            message_data = f"CO_ID = {company_id}, CO_SP_RATE = {sp_rating}"
            dim_message = DimMessage(
                PTS=cmp_record.PTS,
                RecType=cmp_record.RecType,
                MessageSource=message_source,
                MessageType=message_type,
                MessageText=message_text,
                MessageData=message_data
            )
            dim_messages.append(dim_message)

            # Set SPRating and isLowGrade to None
            sp_rating = None
            is_low_grade = None

        # Create DimCompany record
        dim_company_record = DimCompany(
            CompanyID=company_id,
            Name=name,
            SPRating=sp_rating,
            CEO=ceo,
            Description=description,
            FoundingDate=founding_date,
            AddressLine1=address_line1,
            AddressLine2=address_line2,
            PostalCode=postal_code,
            City=city,
            State_Prov=state_prov,
            Country=country,
            Status=status,
            Industry=industry,
            isLowGrade=is_low_grade,
            IsCurrent=is_current,
            EffectiveDate=effective_date,
            EndDate=end_date,
            BatchID=batch_id
        )

        dim_company_records.append(dim_company_record)

    return dim_company_records, dim_messages

@dataclass
class Financial:
    SK_FinancialID: Optional[int]  # Add a surrogate key for the Financial table
    SK_CompanyID: Optional[int]
    FI_YEAR: Optional[str]
    FI_QTR: Optional[str]
    FI_QTR_START_DATE: Optional[str]
    FI_REVENUE: Optional[str]
    FI_NET_EARN: Optional[str]
    FI_BASIC_EPS: Optional[str]
    FI_DILUT_EPS: Optional[str]
    FI_MARGIN: Optional[str]
    FI_INVENTORY: Optional[str]
    FI_ASSETS: Optional[str]
    FI_LIABILITY: Optional[str]
    FI_OUT_BASIC: Optional[str]
    FI_OUT_DILUT: Optional[str]
    PTS: datetime.datetime
    BatchID: Optional[int]

# Function to parse Financial records and return Financial objects
def parse_financial_records(fin_records: list[FINRecord], dim_company_records: list[DimCompany]) -> list[Financial]:
    financial_records = []
    sk = 0
    for fin_record in fin_records:
        sk += 1
        # Extract relevant fields from FINRecord
        year = fin_record.Year.strip() or None
        quarter = fin_record.Quarter.strip() or None
        qtr_start_date = fin_record.QtrStartDate.strip() or None
        revenue = fin_record.Revenue.strip() or None
        net_earn = fin_record.Earnings.strip() or None
        basic_eps = fin_record.EPS.strip() or None
        dilut_eps = fin_record.DilutedEPS.strip() or None
        margin = fin_record.Margin.strip() or None
        inventory = fin_record.Inventory.strip() or None
        assets = fin_record.Assets.strip() or None
        liability = fin_record.Liabilities.strip() or None
        out_basic = fin_record.ShOut.strip() or None
        out_dilut = fin_record.DilutedShOut.strip() or None
        pts = fin_record.PTS
        co_name_or_cik = fin_record.CoNameOrCIK.strip() or None
        batch_id = 1  # Set BatchID as described in section 4.4.2

        # Determine SK_CompanyID by matching CoNameOrCIK in DimCompany records
        sk_company_id = next((dim_company.SK_CompanyID for dim_company in dim_company_records
                                if dim_company.EffectiveDate <= pts < dim_company.EndDate
                                and (dim_company.Name == co_name_or_cik or dim_company.CompanyID == co_name_or_cik)), None)

        # Create Financial record
        financial_record = Financial(
            SK_FinancialID=sk,  # Add a surrogate key for the Financial table
            SK_CompanyID=sk_company_id,
            FI_YEAR=year,
            FI_QTR=quarter,
            FI_QTR_START_DATE=qtr_start_date,
            FI_REVENUE=revenue,
            FI_NET_EARN=net_earn,
            FI_BASIC_EPS=basic_eps,
            FI_DILUT_EPS=dilut_eps,
            FI_MARGIN=margin,
            FI_INVENTORY=inventory,
            FI_ASSETS=assets,
            FI_LIABILITY=liability,
            FI_OUT_BASIC=out_basic,
            FI_OUT_DILUT=out_dilut,
            PTS=pts,
            BatchID=batch_id
        )

        financial_records.append(financial_record)

    return financial_records

@dataclass
class DimSecurity:
    SK_SecurityID: Optional[int]  # Add a surrogate key for the DimSecurity table
    SK_CompanyID: Optional[int]
    Symbol: str
    Issue: str
    Name: str
    ExchangeID: str
    SharesOutstanding: str
    FirstTrade: str
    FirstTradeOnExchange: str
    Dividend: str
    Status: str
    IsCurrent: bool
    EffectiveDate: datetime.datetime
    EndDate: datetime.datetime
    BatchID: int

# Function to parse DimSecurity records and return DimSecurity objects
def parse_dim_security_records(sec_records: list[SECRecord], dim_company_records: list[DimCompany], status_types: list[StatusType]) -> list[DimSecurity]:
    dim_security_records = []
    sk = 0
    for sec_record in sec_records:
        sk += 1
        # Extract relevant fields from SECRecord
        symbol = sec_record.Symbol.strip() or None
        issue = sec_record.IssueType.strip() or None
        name = sec_record.Name.strip() or None
        exchange_id = sec_record.ExID.strip() or None
        shares_outstanding = sec_record.ShOut.strip() or None
        first_trade = sec_record.FirstTradeDate.strip() or None
        first_trade_on_exchange = sec_record.FirstTradeExchg.strip() or None
        dividend = sec_record.Dividend.strip() or None
        pts = sec_record.PTS
        co_name_or_cik = sec_record.CoNameOrCIK.strip() or None
        status = sec_record.Status.strip() or None
        batch_id = 1  # Set BatchID as described in section 4.4.2

        # Determine SK_CompanyID by matching CoNameOrCIK in DimCompany records
        sk_company_id = next((dim_company.SK_CompanyID for dim_company in dim_company_records
                              if dim_company.EffectiveDate <= pts < dim_company.EndDate
                              and (dim_company.Name == co_name_or_cik or dim_company.CompanyID == co_name_or_cik)), None)

        # Determine Status
        status_info = next((st.ST_NAME for st in status_types if st.ST_ID == status), None)

        # Determine IsCurrent, EffectiveDate, and EndDate
        is_current, effective_date, end_date = True, pts, datetime.datetime(9999, 12, 31)

        # Create DimSecurity record
        dim_security_record = DimSecurity(
            SK_SecurityID=sk,  # Add a surrogate key for the DimSecurity table
            SK_CompanyID=sk_company_id,
            Symbol=symbol,
            Issue=issue,
            Name=name,
            ExchangeID=exchange_id,
            SharesOutstanding=shares_outstanding,
            FirstTrade=first_trade,
            FirstTradeOnExchange=first_trade_on_exchange,
            Dividend=dividend,
            Status=status_info,
            IsCurrent=is_current,
            EffectiveDate=effective_date,
            EndDate=end_date,
            BatchID=batch_id
        )

        dim_security_records.append(dim_security_record)

    return dim_security_records

file_path = "./data/data/FINWIRE1970Q1"
dim_company_records = []
for file in os.listdir("data/data"):
    if file.startswith("FINWIRE") and not file.endswith("csv"):
        dim_company_records.extend(parse_file(file_path))

dim_company_records = parse_financial_records([record for record in dim_company_records if record.RecType == "FIN"], [])
print(len(dim_company_records))
# for record in dim_company_records:
#    print(record)
