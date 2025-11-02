import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from rapidfuzz import fuzz, process
import logging
from typing import List, Optional

# Configure logging early so messages are visible
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Constants
URLS = [
    'https://www.canada.ca/content/dam/canada/json/sm-en.json',
    'https://www.canada.ca/content/dam/canada/json/sm-fr.json'
]
REQUEST_TIMEOUT = 10  # seconds
HEADERS = {'User-Agent': 'GC-Socials-DataFetcher/1.0 (+https://github.com/PatLittle/GC-Socials)'}
FUZZ_THRESHOLD = 70  # threshold for fuzzy matching


def safe_get(lst: List, idx: int, default=None):
    try:
        return lst[idx]
    except Exception:
        return default


def extract_href_from_html(html_fragment: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html_fragment or '', 'html.parser')
        a = soup.find('a')
        if a and a.has_attr('href'):
            return a['href']
    except Exception as e:
        logging.debug("Error extracting href: %s", e)
    return None


def load_orgs(path='gc_orgs_with_wikidata_ids.csv') -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding='utf-8')
        logging.info("Loaded %d organizations from %s", len(df), path)
        return df
    except FileNotFoundError:
        logging.warning("%s not found. Proceeding without organization data.", path)
        return pd.DataFrame(columns=['harmonized_name', 'gc_orgID', 'wikidata_id'])
    except Exception as e:
        logging.exception("Failed to load organization CSV: %s", e)
        return pd.DataFrame(columns=['harmonized_name', 'gc_orgID', 'wikidata_id'])


def build_harmonized_lookup(orgs_df: pd.DataFrame):
    """
    Builds a lookup list and mapping for fuzzy matching.
    Returns (choices_list, mapping_name_to_meta)
    """
    choices = []
    mapping = {}
    if orgs_df is None or orgs_df.empty:
        return choices, mapping

    for _, row in orgs_df.iterrows():
        name = row.get('harmonized_name')
        if pd.isna(name):
            continue
        name_str = str(name).strip()
        if not name_str:
            continue
        choices.append(name_str)
        mapping[name_str] = {
            'gc_orgID': row.get('gc_orgID'),
            'wikidata_id': row.get('wikidata_id')
        }
    return choices, mapping


def fetch_and_process(urls=URLS, orgs_df=None):
    combined_data = []
    choices, mapping = build_harmonized_lookup(orgs_df)

    for url in urls:
        logging.info("Requesting %s", url)
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error("Failed to fetch %s: %s", url, e)
            continue

        try:
            data = response.json()
        except ValueError as e:
            logging.error("JSON decoding failed for %s: %s", url, e)
            continue

        aa_data = data.get('aaData', [])
        if not isinstance(aa_data, list):
            logging.warning("Unexpected aaData structure in %s; skipping", url)
            continue

        for idx, record in enumerate(aa_data):
            try:
                if not isinstance(record, (list, tuple)):
                    logging.debug("Skipping non-list record at %s index %d", url, idx)
                    continue

                cleaned_record = []
                # Everything except the last field: strip HTML and keys like 'Department: '
                for field in record[:-1]:
                    if isinstance(field, str):
                        soup = BeautifulSoup(field, 'html.parser')
                        text = soup.get_text(strip=True)
                        cleaned_text = re.sub(r'^(Department|Language|Langue|Ministère):\s*', '', text)
                        cleaned_record.append(cleaned_text)
                    else:
                        cleaned_record.append(field)

                # Extract href from the last field safely
                last_field = record[-1] if len(record) > 0 else ''
                link_url = extract_href_from_html(last_field)
                cleaned_record.append(link_url)

                # Safe department extraction (3rd column expected but may not exist)
                department = safe_get(cleaned_record, 2, None)
                gc_orgID = None
                wikidata_id = None

                if department and choices:
                    # Use rapidfuzz.process.extractOne to get the best candidate from the choices
                    try:
                        result = process.extractOne(str(department), choices, scorer=fuzz.token_sort_ratio)
                        if result:
                            # rapidfuzz.extractOne returns (match, score, index) in many versions
                            # Unpack defensively
                            if len(result) == 3:
                                match, score, _ = result
                            elif len(result) == 2:
                                match, score = result
                            else:
                                match = result[0]
                                score = result[1] if len(result) > 1 else 0

                            if score >= FUZZ_THRESHOLD:
                                meta = mapping.get(match)
                                if meta:
                                    gc_orgID = meta.get('gc_orgID')
                                    wikidata_id = meta.get('wikidata_id')
                    except Exception as e:
                        logging.debug("Fuzzy matching failed for department '%s': %s", department, e)

                # Ensure we have exactly the expected number of columns when appending
                # Expected columns: Account, Platform, Department, Language, URL, gc_orgID, wikidata_id
                record_row = list(cleaned_record[:5])  # take up to the first 5 cleaned fields
                # pad if fewer than 5
                while len(record_row) < 5:
                    record_row.append(None)
                record_row.extend([gc_orgID, wikidata_id])
                combined_data.append(record_row)
            except Exception as e:
                logging.exception("Error processing record index %d from %s: %s", idx, url, e)
                continue

    return combined_data


def write_outputs(combined_data):
    df = pd.DataFrame(combined_data,
                      columns=['Account', 'Platform', 'Department', 'Language', 'URL', 'gc_orgID', 'wikidata_id'])

    # Drop duplicates by URL (safe if URL is None -> will not be deduped)
    df = df.drop_duplicates(subset='URL')

    # Write main CSV
    df.to_csv('sm.csv', index=False, encoding='utf-8')
    logging.info("Wrote sm.csv (%d rows)", len(df))

    # Metrics by platform and language
    current_date = datetime.now().strftime('%Y-%m-%d')
    try:
        platform_counts = df.groupby(['Platform', 'Language']).size().reset_index(name='Count')
    except Exception as e:
        logging.exception("Failed to compute platform counts: %s", e)
        platform_counts = pd.DataFrame(columns=['Platform', 'Language', 'Count'])

    platform_counts['Date'] = current_date

    # Append to existing platform_counts.csv or create it
    try:
        existing_df = pd.read_csv('platform_counts.csv', encoding='utf-8')
        platform_counts = pd.concat([existing_df, platform_counts], ignore_index=True)
        # Optional: dedupe by (Date, Platform, Language) if you don't want multiple identical rows
        # platform_counts = platform_counts.drop_duplicates(subset=['Date', 'Platform', 'Language'])
    except FileNotFoundError:
        pass
    platform_counts.to_csv('platform_counts.csv', index=False, encoding='utf-8')
    logging.info("Wrote platform_counts.csv (%d rows)", len(platform_counts))

    # Department counts
    try:
        department_counts = df['Department'].value_counts().reset_index()
        department_counts.columns = ['Department Name', 'Count']
    except Exception as e:
        logging.exception("Failed to compute department counts: %s", e)
        department_counts = pd.DataFrame(columns=['Department Name', 'Count'])

    department_counts['Date'] = current_date

    try:
        existing_department_df = pd.read_csv('department_counts.csv', encoding='utf-8')
        department_counts = pd.concat([existing_department_df, department_counts], ignore_index=True)
        # Optional dedupe:
        # department_counts = department_counts.drop_duplicates(subset=['Date', 'Department Name'])
    except FileNotFoundError:
        pass
    department_counts.to_csv('department_counts.csv', index=False, encoding='utf-8')
    logging.info("Wrote department_counts.csv (%d rows)", len(department_counts))


def main():
    orgs_df = load_orgs()
    combined_data = fetch_and_process(URLS, orgs_df=orgs_df)
    write_outputs(combined_data)
    logging.info("CSV files 'sm.csv', 'platform_counts.csv', and 'department_counts.csv' have been created successfully.")


if __name__ == '__main__':
    main()
