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
        # The last field contains the link information
        for field in record[:-1]:
            # Remove HTML tags using BeautifulSoup
            soup = BeautifulSoup(field, 'html.parser')
            text = soup.get_text(strip=True)
            # Remove keys (e.g., 'Department: ', 'Language: ')
            cleaned_text = re.sub(r'^(Department|Language|Langue|Ministère):\s*', '', text)
            cleaned_record.append(cleaned_text)
        # Extract href link from the last field
        link_field = BeautifulSoup(record[-1], 'html.parser')
        link_url = link_field.a['href'] if link_field.a and link_field.a.has_attr('href') else None
        cleaned_record.append(link_url)
        combined_data.append(cleaned_record)

# Convert combined JSON to DataFrame, dedupe by URL, and then to CSV
df = pd.DataFrame(combined_data, columns=['Account', 'Platform', 'Department', 'Language', 'URL'])
df = df.drop_duplicates(subset='URL')
df.to_csv('sm.csv', index=False)

print("CSV file 'sm.csv' has been created successfully.")
