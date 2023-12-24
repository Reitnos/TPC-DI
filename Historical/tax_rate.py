from typing import NamedTuple

# Define the TaxRate NamedTuple
class TaxRate(NamedTuple):
    tx_id: str
    tx_name: str
    tx_rate: float

# Function to parse TaxRate.txt
def parse_tax_rate_file(file_path: str) -> list[TaxRate]:
    tax_rates = []

    with open(file_path) as file:
        for line in file:
            fields = line.strip().split('|')

            # Extract fields and create a TaxRate NamedTuple
            tx_id, tx_name, tx_rate = fields
            tax_rate = TaxRate(tx_id, tx_name, float(tx_rate))

            # Append the TaxRate NamedTuple to the list
            tax_rates.append(tax_rate)

    return tax_rates

# Example usage:
# Replace 'TaxRate.txt' with the actual file path
tax_rate_file_path = 'data/data/TaxRate.txt'
tax_rates = parse_tax_rate_file(tax_rate_file_path)

# Print the parsed TaxRate records
for tax_rate in tax_rates:
    print(tax_rate)
