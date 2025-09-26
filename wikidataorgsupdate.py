
import pandas as pd
import requests
import time
import hashlib
from datetime import datetime
from SPARQLWrapper import SPARQLWrapper, JSON
from fuzzywuzzy import fuzz
import logging
from requests.exceptions import RequestException
import os

# Set up logging
log_filename = 'wikidata_orgs_update.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Initialize SPARQLWrapper for Wikidata
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(JSON)
sparql.setTimeout(30)

# File paths
HISTORICAL_CSV = 'gc_orgs_with_wikidata_ids.csv'
CURRENT_CSV = 'gc_orgs_current.csv'
API_RESOURCE_ID = 'cb5b5566-f599-4d12-abae-8279a0230928'

def fetch_all_orgs_from_api():
    """Fetch all organizations from the API with pagination."""
    base_url = 'https://open.canada.ca/data/en/api/3/action/datastore_search'
    limit = 100
    offset = 0
    all_records = []

    while True:
        url = f"{base_url}?resource_id={API_RESOURCE_ID}&limit={limit}&offset={offset}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data['success']:
                logging.error("API request failed.")
                return None
            records = data['result']['records']
            all_records.extend(records)
            total = data['result']['total']
            logging.info(f"Fetched {len(records)} records, total so far: {len(all_records)}/{total}")
            if len(all_records) >= total:
                break
            offset += limit
            time.sleep(0.5)
        except RequestException as e:
            logging.error(f"Error fetching data from API at offset {offset}: {e}")
            return None

    df = pd.DataFrame(all_records)
    logging.info(f"Successfully fetched {len(df)} organizations from API")
    return df

def load_existing_csv(filename):
    """Load existing CSV if it exists, return empty DataFrame if not."""
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename)
            logging.info(f"Loaded {len(df)} existing records from {filename}")
            return df
        except Exception as e:
            logging.warning(f"Could not load {filename}: {e}. Starting with empty DataFrame.")
    else:
        logging.info(f"{filename} does not exist. Starting with empty DataFrame.")
    return pd.DataFrame()

def generate_row_hash(row):
    """Generate a unique hash for a row based on key fields."""
    key_fields = ['gc_orgID', 'harmonized_name', 'legal_title', 'preferred_name', 'status_statut']
    hash_input = ''
    for field in key_fields:
        value = row.get(field, '')
        if pd.notna(value):
            hash_input += str(value)
    return hashlib.md5(hash_input.encode()).hexdigest()[:8]

def identify_new_orgs(api_df, existing_df):
    """Identify new organizations by comparing gc_orgID."""
    if existing_df.empty:
        logging.info("No existing CSV found. All API organizations are new.")
        return api_df
    
    existing_ids = set(existing_df['gc_orgID'].astype(str))
    api_ids = set(api_df['gc_orgID'].astype(str))
    
    new_ids = api_ids - existing_ids
    if not new_ids:
        logging.info("No new organizations found.")
        return pd.DataFrame()
    
    new_orgs = api_df[api_df['gc_orgID'].astype(str).isin(new_ids)].copy()
    logging.info(f"Found {len(new_orgs)} new organizations: {list(new_ids)}")
    return new_orgs

def escape_sparql_string(s):
    """Escape special characters in SPARQL string."""
    if pd.isna(s):
        return ''
    return s.replace('"', '\\"').replace('\n', '').replace('\r', '').replace('\\', '\\\\')

def get_wikidata_candidates(row, retries=3, delay=2):
    """Query Wikidata for matches across name fields in one query."""
    names = [
        row['harmonized_name'] if pd.notna(row['harmonized_name']) else None,
        row['legal_title'] if pd.notna(row['legal_title']) else None,
        row['preferred_name'] if pd.notna(row['preferred_name']) else None
    ]
    names = [name for name in names if name]
    if not names:
        return []

    # Build SPARQL query parts for each name
    query_parts = []
    for name in names:
        cleaned_name = escape_sparql_string(name)
        if cleaned_name:
            query_parts.append(f"""
                {{ ?item rdfs:label ?itemLabel .
                   FILTER(LANG(?itemLabel) = "en")
                   FILTER(CONTAINS(LCASE(?itemLabel), LCASE("{cleaned_name}")))
                }}
                UNION
                {{ ?item skos:altLabel ?alias .
                   FILTER(LANG(?alias) = "en")
                   FILTER(CONTAINS(LCASE(?alias), LCASE("{cleaned_name}")))
                }}
            """)

    if not query_parts:
        return []

    # Construct the full SPARQL query
    query = f"""
    SELECT DISTINCT ?item ?itemLabel ?alias
    WHERE {{
      ?item wdt:P31/wdt:P279* wd:Q327333 . # Instance or subclass of government agency
      ?item wdt:P17 wd:Q16 . # Country: Canada
      {" UNION ".join(query_parts)}
    }}
    LIMIT 10
    """

    for attempt in range(retries):
        sparql.setQuery(query)
        try:
            results = sparql.query().convert()
            bindings = results['results']['bindings']
            candidates = []
            for binding in bindings:
                wikidata_id = binding['item']['value'].split('/')[-1]
                label = binding.get('itemLabel', {}).get('value', '')
                alias = binding.get('alias', {}).get('value', '')
                candidates.append({
                    'wikidata_id': wikidata_id,
                    'label': label,
                    'alias': alias
                })
            if candidates:
                logging.info(f"Retrieved {len(candidates)} candidates for org {row.get('gc_orgID', 'unknown')}")
            else:
                logging.warning(f"No candidates found for org {row.get('gc_orgID', 'unknown')}")
            return candidates
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{retries} failed for org {row.get('gc_orgID', 'unknown')}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            continue
    
    logging.error(f"All {retries} attempts failed for org {row.get('gc_orgID', 'unknown')}")
    return []

def calculate_match_probability(input_name, candidate_label, candidate_alias):
    """Calculate the match probability using fuzzy matching."""
    if not input_name or (not candidate_label and not candidate_alias):
        return 0
    scores = []
    if candidate_label:
        scores.append(fuzz.token_sort_ratio(input_name.lower(), candidate_label.lower()))
    if candidate_alias:
        scores.append(fuzz.token_sort_ratio(input_name.lower(), candidate_alias.lower()))
    return max(scores) if scores else 0

def get_wikidata_mapping(row):
    """Find the best Wikidata ID for a given organization row."""
    names = [
        ('harmonized_name', row['harmonized_name'] if pd.notna(row['harmonized_name']) else None),
        ('legal_title', row['legal_title'] if pd.notna(row['legal_title']) else None),
        ('preferred_name', row['preferred_name'] if pd.notna(row['preferred_name']) else None)
    ]
    names = [(field, name) for field, name in names if name]

    best_match = {'wikidata_id': None, 'probability': 0, 'matched_name': None, 'matched_field': None}
    min_probability_threshold = 70

    candidates = get_wikidata_candidates(row)
    for candidate in candidates:
        for field, name in names:
            probability = calculate_match_probability(name, candidate['label'], candidate['alias'])
            if probability > best_match['probability'] and probability >= min_probability_threshold:
                best_match = {
                    'wikidata_id': candidate['wikidata_id'],
                    'probability': probability,
                    'matched_name': name,
                    'matched_field': field
                }

    if best_match['wikidata_id']:
        logging.info(
            f"✓ Matched {row.get('gc_orgID', 'unknown')}: {best_match['wikidata_id']} "
            f"({best_match['probability']}%) via {best_match['matched_field']}"
        )
    else:
        logging.warning(f"✗ No match for {row.get('gc_orgID', 'unknown')} above {min_probability_threshold}%")

    return best_match['wikidata_id'], best_match['probability'], best_match['matched_name'], best_match['matched_field']

def process_new_orgs(new_orgs_df):
    """Process new organizations and add Wikidata mappings."""
    if new_orgs_df.empty:
        return pd.DataFrame()

    logging.info(f"Processing {len(new_orgs_df)} new organizations...")
    processed_rows = []

    for index, row in new_orgs_df.iterrows():
        row_data = row.to_dict()
        row_data['update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_data['row_hash'] = generate_row_hash(row_data)

        wikidata_id, probability, matched_name, matched_field = get_wikidata_mapping(row)
        row_data['wikidata_id'] = wikidata_id
        row_data['match_probability'] = probability
        row_data['matched_name'] = matched_name
        row_data['matched_field'] = matched_field

        processed_rows.append(row_data)
        time.sleep(1)

    processed_df = pd.DataFrame(processed_rows)
    logging.info(f"Processed {len(processed_df)} new organizations with Wikidata mappings")
    return processed_df

def update_historical_csv(existing_df, new_orgs_df):
    """Update the historical CSV by appending new organizations."""
    if new_orgs_df.empty:
        logging.info("No new organizations to append to historical CSV")
        return existing_df

    for col in ['update_date', 'row_hash', 'wikidata_id', 'match_probability', 'matched_name', 'matched_field']:
        if col not in existing_df.columns:
            existing_df[col] = None

    updated_df = pd.concat([existing_df, new_orgs_df], ignore_index=True)
    
    updated_df.to_csv(HISTORICAL_CSV, index=False)
    logging.info(f"Updated historical CSV with {len(new_orgs_df)} new records. Total: {len(updated_df)}")
    
    return updated_df

def update_current_csv(historical_df):
    """Update the current-state CSV with only active organizations."""
    active_orgs = historical_df[historical_df['status_statut'] == 'a'].copy()
    current_columns = [col for col in historical_df.columns if col not in ['update_date', 'row_hash']]
    active_orgs = active_orgs[current_columns]
    
    active_orgs.to_csv(CURRENT_CSV, index=False)
    logging.info(f"Updated current CSV with {len(active_orgs)} active organizations")
    
    return active_orgs

def main():
    """Main execution function."""
    logging.info("=== Starting Wikidata Organizations Update ===")
    
    logging.info("Step 1: Fetching organizations from Open Canada API...")
    api_df = fetch_all_orgs_from_api()
    if api_df is None:
        logging.error("Failed to fetch organizations from API. Exiting.")
        return 1

    logging.info("Step 2: Loading existing CSV...")
    existing_df = load_existing_csv(HISTORICAL_CSV)

    logging.info("Step 3: Identifying new organizations...")
    new_orgs_df = identify_new_orgs(api_df, existing_df)
    
    if new_orgs_df.empty:
        logging.info("No new organizations found. Update complete.")
        if not existing_df.empty:
            update_current_csv(existing_df)
        logging.info("=== Update Complete: No Changes ===")
        return 0

    logging.info("Step 4: Processing new organizations with Wikidata mapping...")
    processed_new_orgs = process_new_orgs(new_orgs_df)

    logging.info("Step 5: Updating historical CSV...")
    updated_historical_df = update_historical_csv(existing_df, processed_new_orgs)

    logging.info("Step 6: Updating current-state CSV...")
    update_current_csv(updated_historical_df)

    logging.info(f"=== Update Complete: Added {len(processed_new_orgs)} new organizations ===")
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
