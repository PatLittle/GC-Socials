import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime

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

# Create a CSV with date and count of each platform
current_date = datetime.now().strftime('%Y-%m-%d')
platform_counts = df['Platform'].value_counts()
platform_counts['Date'] = current_date

# Convert to DataFrame and reorder columns
platform_df = platform_counts.reset_index().pivot(index=None, columns='index', values='Platform').fillna(0)
platform_df.insert(0, 'Date', current_date)

# Append to the existing platform_counts CSV or create a new one if it doesn't exist
try:
    existing_df = pd.read_csv('platform_counts.csv')
    platform_df = pd.concat([existing_df, platform_df], ignore_index=True)
except FileNotFoundError:
    pass

platform_df.to_csv('platform_counts.csv', index=False)

# Create a CSV with date, department name, and count (long data format)
department_counts = df['Department'].value_counts().reset_index()
department_counts.columns = ['Department Name', 'Count']
department_counts['Date'] = current_date

# Append to the existing department_counts CSV or create a new one if it doesn't exist
try:
    existing_department_df = pd.read_csv('department_counts.csv')
    department_counts = pd.concat([existing_department_df, department_counts], ignore_index=True)
except FileNotFoundError:
    pass

department_counts.to_csv('department_counts.csv', index=False)

print("CSV files 'sm.csv', 'platform_counts.csv', and 'department_counts.csv' have been created successfully.")
