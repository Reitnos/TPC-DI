from typing import List, NamedTuple

# Define the Industry NamedTuple
class Industry(NamedTuple):
    IN_ID: str
    IN_NAME: str
    IN_SC_ID: str

# Function to process Industry records from Industry.txt
def process_industry_records(file_path: str) -> List[Industry]:
    industries = []

    # Open the file and read lines
    with open(file_path, 'r') as file:
        for line in file:
            # Split fields by vertical bar ("|")
            fields = line.strip().split('|')

            # Extract values and create Industry record
            in_id, in_name, in_sc_id = fields
            industry = Industry(IN_ID=in_id, IN_NAME=in_name, IN_SC_ID=in_sc_id)
            industries.append(industry)

    return industries

# Example usage
industry_file_path = "data/data/Industry.txt"
industry_result = process_industry_records(industry_file_path)

# Print the result
for industry in industry_result:
    print(industry)
