import requests
import pandas as pd
import re
from bs4 import BeautifulSoup

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
    aa_data = data.get('aaData', [])

    for record in aa_data:
        cleaned_record = []
        for field in record:
            # Remove HTML tags using BeautifulSoup
            soup = BeautifulSoup(field, 'html.parser')
            cleaned_record.append(soup.get_text(strip=True))
        combined_data.append(cleaned_record)

# Convert combined JSON to DataFrame and then to CSV
df = pd.DataFrame(combined_data, columns=['Account', 'Platform', 'Department', 'Language', 'Link'])
df.to_csv('sm.csv', index=False)

print("CSV file 'sm.csv' has been created successfully.")
