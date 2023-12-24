from typing import NamedTuple
from datetime import datetime
import csv

class TimeRecord(NamedTuple):
    SK_TimeID: int
    TimeValue: str
    HourID: int
    HourDesc: str
    MinuteID: int
    MinuteDesc: str
    SecondID: int
    SecondDesc: str
    MarketHoursFlag: bool
    OfficeHoursFlag: bool

# Function to parse a line of the Time.txt file and return a TimeRecord
def parse_time_line(line):
    fields = line.strip().split('|')
    return TimeRecord(
        SK_TimeID=int(fields[0]),
        TimeValue=fields[1],
        HourID=int(fields[2]),
        HourDesc=fields[3],
        MinuteID=int(fields[4]),
        MinuteDesc=fields[5],
        SecondID=int(fields[6]),
        SecondDesc=fields[7],
        MarketHoursFlag=bool(fields[8]),
        OfficeHoursFlag=bool(fields[9])
    )

# Function to load DimTime table from Time.txt file
def load_dim_time(file_path):
    dim_time_records = []

    with open(file_path) as file:
        for line in file:
            dim_time_record = parse_time_line(line)
            dim_time_records.append(dim_time_record)

    return dim_time_records

# Example usage
file_path = 'data/data/Time.txt'
dim_time_data = load_dim_time(file_path)
print(dim_time_data)
# Now, you can use the dim_time_data list to insert records into your database.
# Note: Adjust the database insertion logic based on your specific database interface.
