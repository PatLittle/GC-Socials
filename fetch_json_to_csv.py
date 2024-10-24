import requests
import pandas as pd

# Fetch the JSON data (English and French)
urls = [
    'https://www.canada.ca/content/dam/canada/json/sm-en.json',
    'https://www.canada.ca/content/dam/canada/json/sm-fr.json'
]

# Combine data from both URLs
combined_data = []
for url in urls:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    combined_data.extend(data if isinstance(data, list) else [data])

# Convert combined JSON to DataFrame and then to CSV
df = pd.json_normalize(combined_data)
df.to_csv('sm.csv', index=False)

print("CSV file 'sm.csv' has been created successfully.")
