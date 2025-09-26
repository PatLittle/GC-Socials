import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from fuzzywuzzy import fuzz
import logging

# Fetch the JSON data (English and French)
urls = [
    'https://www.canada.ca/content/dam/canada/json/sm-en.json',
    'https://www.canada.ca/content/dam/canada/json/sm-fr.json'
]

# Load gc_orgs_with_wikidata_ids.csv
try:
    orgs_df = pd.read_csv('gc_orgs_with_wikidata_ids.csv')
    logging.info(f"Loaded {len(orgs_df)} organizations from gc_orgs_with_wikidata_ids.csv")
except FileNotFoundError:
    logging.warning("gc_orgs_with_wikidata_ids.csv not found. Proceeding without organization data.")
    orgs_df = pd.DataFrame(columns=['harmonized_name', 'gc_orgID', 'wikidata_id'])

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
            if isinstance(field, str):
                soup = BeautifulSoup(field, 'html.parser')
                text = soup.get_text(strip=True)
                # Remove keys (e.g., 'Department: ', 'Language: ')
                cleaned_text = re.sub(r'^(Department|Language|Langue|Ministère):\s*', '', text)
                cleaned_record.append(cleaned_text)
            else:
                cleaned_record.append(field)
        # Extract href link from the last field
        link_field = BeautifulSoup(record[-1], 'html.parser')
        link_url = link_field.a['href'] if link_field.a and link_field.a.has_attr('href') else None
        cleaned_record.append(link_url)
        
        # Match department to gc_orgs_with_wikidata_ids.csv using fuzzy matching
        department = cleaned_record[2]  # Department is the third column
        gc_orgID = None
        wikidata_id = None
        if department and not orgs_df.empty:
            # Find best match for department in harmonized_name
            best_match = None
            highest_score = 0
            for _, org_row in orgs_df.iterrows():
                harmonized_name = org_row['harmonized_name']
                if pd.notna(harmonized_name):
                    score = fuzz.token_sort_ratio(department.lower(), harmonized_name.lower())
                    if score > highest_score and score >= 70:  # Threshold for matching
                        highest_score = score
                        best_match = org_row
            if best_match is not None:
                gc_orgID = best_match['gc_orgID']
                wikidata_id = best_match['wikidata_id']
        
        cleaned_record.extend([gc_orgID, wikidata_id])
        combined_data.append(cleaned_record)

# Convert combined JSON to DataFrame, dedupe by URL, and then to CSV
df = pd.DataFrame(combined_data, columns=['Account', 'Platform', 'Department', 'Language', 'URL', 'gc_orgID', 'wikidata_id'])
df = df.drop_duplicates(subset='URL')
df.to_csv('sm.csv', index=False)

# Create a CSV with date, count of each platform, and language
current_date = datetime.now().strftime('%Y-%m-%d')
platform_counts = df.groupby(['Platform', 'Language']).size().reset_index(name='Count')
platform_counts['Date'] = current_date

# Append to the existing platform_counts CSV or create a new one if it doesn't exist
try:
    existing_df = pd.read_csv('platform_counts.csv')
    platform_counts = pd.concat([existing_df, platform_counts], ignore_index=True)
except FileNotFoundError:
    pass

platform_counts.to_csv('platform_counts.csv', index=False)

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
