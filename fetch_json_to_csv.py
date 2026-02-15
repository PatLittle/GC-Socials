import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from rapidfuzz import fuzz, process
import logging
from typing import List, Optional
import subprocess
import io
import os

RECENT_CHANGES_SNIPPET_PATH = 'recent_changes_snippet.md'

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
                for field in record[:-1]:
                    if isinstance(field, str):
                        soup = BeautifulSoup(field, 'html.parser')
                        text = soup.get_text(strip=True)
                        cleaned_text = re.sub(r'^(Department|Language|Langue|Ministère):\s*', '', text)
                        cleaned_record.append(cleaned_text)
                    else:
                        cleaned_record.append(field)

                last_field = record[-1] if len(record) > 0 else ''
                link_url = extract_href_from_html(last_field)
                cleaned_record.append(link_url)

                department = safe_get(cleaned_record, 2, None)
                gc_orgID = None
                wikidata_id = None

                if department and choices:
                    try:
                        result = process.extractOne(str(department), choices, scorer=fuzz.token_sort_ratio)
                        if result:
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

                record_row = list(cleaned_record[:5])
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

    df = df.drop_duplicates(subset='URL')
    current_date = datetime.now().strftime('%Y-%m-%d')

    existing_dates = {}
    if os.path.exists('sm.csv'):
        try:
            existing_sm = pd.read_csv('sm.csv', encoding='utf-8')
            if {'URL', 'Date Added'}.issubset(existing_sm.columns):
                existing_sm['URL'] = existing_sm['URL'].astype(str).str.strip()
                existing_sm['Date Added'] = existing_sm['Date Added'].astype(str).str.strip()
                existing_dates = {
                    row['URL']: row['Date Added']
                    for _, row in existing_sm[['URL', 'Date Added']].dropna(subset=['URL']).iterrows()
                    if row['URL']
                }
        except Exception as e:
            logging.warning("Unable to load existing Date Added values from sm.csv: %s", e)

    df['URL'] = df['URL'].astype(str).str.strip()
    df['Date Added'] = df['URL'].map(existing_dates)
    df['Date Added'] = df['Date Added'].replace({'': None, 'nan': None})
    df['Date Added'] = df['Date Added'].fillna(current_date)

    df.to_csv('sm.csv', index=False, encoding='utf-8')
    logging.info("Wrote sm.csv (%d rows)", len(df))

    try:
        platform_counts = df.groupby(['Platform', 'Language']).size().reset_index(name='Count')
    except Exception as e:
        logging.exception("Failed to compute platform counts: %s", e)
        platform_counts = pd.DataFrame(columns=['Platform', 'Language', 'Count'])

    platform_counts['Date'] = current_date

    try:
        existing_df = pd.read_csv('platform_counts.csv', encoding='utf-8')
        platform_counts = pd.concat([existing_df, platform_counts], ignore_index=True)
    except FileNotFoundError:
        pass
    platform_counts.to_csv('platform_counts.csv', index=False, encoding='utf-8')
    logging.info("Wrote platform_counts.csv (%d rows)", len(platform_counts))

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
    except FileNotFoundError:
        pass
    department_counts.to_csv('department_counts.csv', index=False, encoding='utf-8')
    logging.info("Wrote department_counts.csv (%d rows)", len(department_counts))


def _find_url_column(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        if str(col).strip().lower() == 'url':
            return col
    return None


def get_deleted_rows_from_git_history(sm_path='sm.csv', output_path='deleted_rows.csv'):
    if not os.path.isdir('.git'):
        logging.warning("Not a git repository (no .git directory found). Skipping git history analysis.")
        return

    try:
        rev_list = subprocess.run(['git', 'rev-list', '--reverse', '--all', '--', sm_path],
                                  capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.warning("Failed to list git revisions for %s: %s", sm_path, e)
        return

    commits = [c.strip() for c in rev_list.stdout.splitlines() if c.strip()]
    if not commits:
        logging.info("No commits found for %s; nothing to do.", sm_path)
        return

    logging.info("Found %d commits touching %s", len(commits), sm_path)

    existing_deleted_df = pd.DataFrame()
    if os.path.exists(output_path):
        try:
            existing_deleted_df = pd.read_csv(output_path, encoding='utf-8')
        except Exception as e:
            logging.warning("Unable to read existing %s for merge: %s", output_path, e)

    previous_df = None
    deleted_rows_accum = []

    for commit in commits:
        try:
            show = subprocess.run(['git', 'show', f'{commit}:{sm_path}'], capture_output=True, text=True)
            if show.returncode != 0:
                continue

            try:
                curr_df = pd.read_csv(io.StringIO(show.stdout), encoding='utf-8')
            except Exception as e:
                logging.debug("Failed to parse sm.csv at commit %s: %s", commit, e)
                continue

            url_col = _find_url_column(curr_df)
            if url_col is None:
                previous_df = curr_df
                continue

            curr_df[url_col] = curr_df[url_col].astype(str).str.strip().replace({'nan': ''})
            curr_urls = {u for u in set(curr_df[url_col].dropna().astype(str).str.strip()) if u}

            if previous_df is not None:
                prev_url_col = _find_url_column(previous_df)
                if prev_url_col is None:
                    previous_df = curr_df
                    continue

                previous_df[prev_url_col] = previous_df[prev_url_col].astype(str).str.strip().replace({'nan': ''})
                prev_urls = {u for u in set(previous_df[prev_url_col].dropna().astype(str).str.strip()) if u}

                deleted_urls = prev_urls - curr_urls
                if deleted_urls:
                    deleted_rows = previous_df[previous_df[prev_url_col].isin(deleted_urls)].copy()
                    try:
                        deleted_date = subprocess.run(
                            ['git', 'show', '-s', '--format=%cs', commit],
                            capture_output=True,
                            text=True,
                            check=True
                        ).stdout.strip()
                    except subprocess.CalledProcessError:
                        deleted_date = ''
                    deleted_rows['Date Deleted'] = deleted_date
                    deleted_rows_accum.append(deleted_rows)

            previous_df = curr_df
        except Exception as e:
            logging.exception("Error while processing commit %s: %s", commit, e)

    if not deleted_rows_accum:
        logging.info("No deleted rows found in git history for %s", sm_path)
        if not existing_deleted_df.empty:
            logging.info("Keeping existing %s (%d rows).", output_path, len(existing_deleted_df))
        return

    try:
        all_deleted = pd.concat(deleted_rows_accum, ignore_index=True)
    except Exception as e:
        logging.exception("Failed to concat deleted rows: %s", e)
        return

    url_col = _find_url_column(all_deleted)
    if url_col is None:
        logging.warning("Deleted rows have no URL column; writing raw deleted rows to %s", output_path)
        all_deleted.to_csv(output_path, index=False, encoding='utf-8')
        return

    if 'Date Deleted' in all_deleted.columns:
        all_deleted['Date Deleted'] = pd.to_datetime(all_deleted['Date Deleted'], errors='coerce')
        all_deleted = all_deleted.sort_values('Date Deleted')
    deduped = all_deleted.drop_duplicates(subset=[url_col], keep='last')

    existing_url_col = _find_url_column(existing_deleted_df) if not existing_deleted_df.empty else None
    if existing_url_col:
        existing_deleted_df[existing_url_col] = existing_deleted_df[existing_url_col].astype(str).str.strip().replace({'nan': ''})
        deduped[url_col] = deduped[url_col].astype(str).str.strip().replace({'nan': ''})
        tracked_urls = set(deduped[url_col])
        preserved_existing = existing_deleted_df[~existing_deleted_df[existing_url_col].isin(tracked_urls)].copy()

        if not preserved_existing.empty:
            for col in deduped.columns:
                if col not in preserved_existing.columns:
                    preserved_existing[col] = None
            for col in preserved_existing.columns:
                if col not in deduped.columns:
                    deduped[col] = None
            deduped = pd.concat([deduped, preserved_existing[deduped.columns]], ignore_index=True)

    if 'Date Deleted' in deduped.columns:
        deduped['Date Deleted'] = pd.to_datetime(deduped['Date Deleted'], errors='coerce').dt.strftime('%Y-%m-%d')
        deduped['Date Deleted'] = deduped['Date Deleted'].fillna('')

    deduped.to_csv(output_path, index=False, encoding='utf-8')
    logging.info("Wrote %s (%d rows) - deduplicated by %s", output_path, len(deduped), url_col)


def generate_recent_changes_snippet(sm_path='sm.csv', deleted_path='deleted_rows.csv', output_path=RECENT_CHANGES_SNIPPET_PATH):
    now = datetime.now()
    lookback_days = 14
    cutoff = now - pd.Timedelta(days=lookback_days)

    def _recent_table(df: pd.DataFrame, date_col: str, title: str) -> str:
        if df.empty or date_col not in df.columns:
            return f"### {title}\n\n_No accounts in the last {lookback_days} days._\n"

        local_df = df.copy()
        local_df[date_col] = pd.to_datetime(local_df[date_col], errors='coerce')
        local_df = local_df[local_df[date_col] >= cutoff].sort_values(date_col, ascending=False)

        columns = [c for c in ['Account', 'Platform', 'Department', 'Language', 'URL', date_col] if c in local_df.columns]
        if local_df.empty or not columns:
            return f"### {title}\n\n_No accounts in the last {lookback_days} days._\n"

        local_df = local_df[columns].copy()
        local_df[date_col] = local_df[date_col].dt.strftime('%Y-%m-%d')
        return f"### {title}\n\n" + local_df.to_markdown(index=False) + "\n"

    try:
        sm_df = pd.read_csv(sm_path, encoding='utf-8')
    except FileNotFoundError:
        sm_df = pd.DataFrame()

    try:
        deleted_df = pd.read_csv(deleted_path, encoding='utf-8')
    except FileNotFoundError:
        deleted_df = pd.DataFrame()

    snippet = (
        "## Recent Account Changes (Last 14 Days)\n\n"
        + _recent_table(sm_df, 'Date Added', 'Accounts Added')
        + "\n"
        + _recent_table(deleted_df, 'Date Deleted', 'Accounts Deleted')
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(snippet)
    logging.info("Wrote %s", output_path)


def main():
    orgs_df = load_orgs()
    combined_data = fetch_and_process(URLS, orgs_df=orgs_df)
    write_outputs(combined_data)

    try:
        get_deleted_rows_from_git_history(sm_path='sm.csv', output_path='deleted_rows.csv')
    except Exception as e:
        logging.exception("Failed to compute deleted rows from git history: %s", e)

    try:
        generate_recent_changes_snippet('sm.csv', 'deleted_rows.csv', RECENT_CHANGES_SNIPPET_PATH)
    except Exception as e:
        logging.exception("Failed to generate %s: %s", RECENT_CHANGES_SNIPPET_PATH, e)

    logging.info(
        "CSV files 'sm.csv', 'platform_counts.csv', 'department_counts.csv', 'deleted_rows.csv', and markdown snippet '%s' have been created successfully.",
        RECENT_CHANGES_SNIPPET_PATH
    )


if __name__ == '__main__':
    main()
