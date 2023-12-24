from typing import NamedTuple
from datetime import datetime
from pathlib import Path

class DateRecord(NamedTuple):
    SK_DateID: int
    DateValue: str
    DateDesc: str
    CalendarYearID: int
    CalendarYearDesc: str
    CalendarQtrID: int
    CalendarQtrDesc: str
    CalendarMonthID: int
    CalendarMonthDesc: str
    CalendarWeekID: int
    CalendarWeekDesc: str
    DayOfWeekNum: int
    DayOfWeekDesc: str
    FiscalYearID: int
    FiscalYearDesc: str
    FiscalQtrID: int
    FiscalQtrDesc: str
    HolidayFlag: bool

def parse_date_file(file_path: str) -> list[DateRecord]:
    date_records = []

    with open(file_path) as file:
        for line in file:
            # Split the line into fields using '|'
            fields = line.strip().split('|')

            # Unpack the fields into variables
            (
                SK_DateID, DateValue, DateDesc, CalendarYearID, CalendarYearDesc,
                CalendarQtrID, CalendarQtrDesc, CalendarMonthID, CalendarMonthDesc,
                CalendarWeekID, CalendarWeekDesc, DayOfWeekNum, DayOfWeekDesc,
                FiscalYearID, FiscalYearDesc, FiscalQtrID, FiscalQtrDesc, HolidayFlag
            ) = fields

            # Convert string values to appropriate types
            SK_DateID = int(SK_DateID)
            CalendarYearID = int(CalendarYearID)
            CalendarQtrID = int(CalendarQtrID)
            CalendarMonthID = int(CalendarMonthID)
            CalendarWeekID = int(CalendarWeekID)
            DayOfWeekNum = int(DayOfWeekNum)
            FiscalYearID = int(FiscalYearID)
            HolidayFlag = bool(HolidayFlag == "true")

            # Create DateRecord instance and append to the list
            date_record = DateRecord(
                SK_DateID=SK_DateID,
                DateValue=DateValue,
                DateDesc=DateDesc,
                CalendarYearID=CalendarYearID,
                CalendarYearDesc=CalendarYearDesc,
                CalendarQtrID=CalendarQtrID,
                CalendarQtrDesc=CalendarQtrDesc,
                CalendarMonthID=CalendarMonthID,
                CalendarMonthDesc=CalendarMonthDesc,
                CalendarWeekID=CalendarWeekID,
                CalendarWeekDesc=CalendarWeekDesc,
                DayOfWeekNum=DayOfWeekNum,
                DayOfWeekDesc=DayOfWeekDesc,
                FiscalYearID=FiscalYearID,
                FiscalYearDesc=FiscalYearDesc,
                FiscalQtrID=FiscalQtrID,
                FiscalQtrDesc=FiscalQtrDesc,
                HolidayFlag=HolidayFlag
            )
            date_records.append(date_record)

    return date_records

# Example usage:
# date_records = parse_date_file('Date.txt')
# for date_record in date_records:
#     print(date_record)
