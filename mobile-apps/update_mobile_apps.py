"""Build a bilingual inventory of Government of Canada mobile applications.

The current CSV is replaced on every run. Apps present in the previous current
CSV but absent from both current Canada.ca feeds are appended to the deletion
history with the date on which their removal was detected.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from html import unescape
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests
from bs4 import BeautifulSoup

ENGLISH_URL = "https://www.canada.ca/content/dam/canada/json/mob-en.json"
FRENCH_URL = "https://www.canada.ca/content/dam/canada/json/mob-fr.json"

COLUMNS = [
    "Name",
    "Nom",
    "Publication date",
    "Platforms",
    "Short description",
    "Courte description",
    "Long description",
    "Longue description",
    "Published by",
    "Publié par",
    "Department",
    "Ministère",
    "Download links",
    "Téléchargements",
    "icon",
]
DELETED_COLUMNS = COLUMNS + ["Removal detected date"]
COUNT_COLUMNS = [
    "date",
    "Department",
    "Ministère",
    "unique_app_count",
    "iOS_count",
    "android_count",
    "blackberry_count",
    "amazon_count",
]
PLATFORM_COUNTS = {
    "ios": "iOS_count",
    "android": "android_count",
    "blackberry": "blackberry_count",
    "amazon": "amazon_count",
}
SANKEY_START = "<!-- MOBILE_APPS_SANKEY_START -->"
SANKEY_END = "<!-- MOBILE_APPS_SANKEY_END -->"


def normalized_space(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def soup_for(value: object) -> BeautifulSoup:
    return BeautifulSoup(str(value or ""), "html.parser")


def text_from_html(value: object) -> str:
    """Return readable plain text from a feed field containing HTML."""
    return normalized_space(soup_for(value).get_text(" ", strip=True))


def app_name(value: object) -> str:
    soup = soup_for(value)
    element = soup.select_one("a.record-expand") or soup.find("a")
    return normalized_space(
        element.get_text(" ", strip=True) if element else soup.get_text(" ", strip=True)
    )


def icon_from_html(value: object) -> str:
    image = soup_for(value).select_one("img.product-icon, img")
    return unescape(image.get("src", "")).strip() if image else ""


def materialize_icon(value: str, icon_directory: Path, output_directory: Path) -> str:
    """Decode an embedded icon and return a stable path suitable for the CSV."""
    match = re.fullmatch(
        r"data:image/(?P<mime>[a-zA-Z0-9.+-]+);base64,(?P<data>.+)",
        value,
        flags=re.DOTALL,
    )
    if not match:
        return value

    try:
        image_bytes = base64.b64decode(match.group("data"), validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("An app icon contains invalid base64 data") from error

    extension = {
        "jpeg": "jpg",
        "svg+xml": "svg",
    }.get(match.group("mime").lower(), match.group("mime").lower())
    if not re.fullmatch(r"[a-z0-9]+", extension):
        raise ValueError(f"Unsupported app icon type: {match.group('mime')}")

    digest = hashlib.sha256(image_bytes).hexdigest()[:20]
    icon_path = icon_directory / f"{digest}.{extension}"
    icon_directory.mkdir(parents=True, exist_ok=True)
    if not icon_path.exists() or icon_path.read_bytes() != image_bytes:
        temporary = icon_path.with_suffix(icon_path.suffix + ".tmp")
        temporary.write_bytes(image_bytes)
        temporary.replace(icon_path)

    try:
        return icon_path.relative_to(output_directory).as_posix()
    except ValueError:
        return icon_path.as_posix()


def platforms_from_html(value: object) -> str:
    soup = soup_for(value)
    values = [
        normalized_space(item.get_text(" ", strip=True))
        for item in soup.select(".label")
    ]
    if not values:
        values = [text_from_html(value)]
    return "; ".join(dict.fromkeys(item for item in values if item))


def publisher_from_html(value: object, language: str) -> str:
    text = text_from_html(value)
    prefix = r"^Published by\s*:\s*" if language == "en" else r"^Publié par\s*:\s*"
    return re.sub(prefix, "", text, flags=re.IGNORECASE)


def download_links(value: object) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for anchor in soup_for(value).select("a.product-link[href], a[href]"):
        url = unescape(anchor.get("href", "")).strip()
        if not url or url.startswith("#"):
            continue
        label = normalized_space(anchor.get_text(" ", strip=True))
        links.append((label, url))
    return links


def format_download_links(value: object) -> str:
    return " | ".join(
        f"{label}: {url}" if label else url for label, url in download_links(value)
    )


def canonical_link_token(url: str) -> str:
    """Extract a language-independent store identifier from a download URL."""
    parsed = urlsplit(unescape(url))
    host = parsed.netloc.lower().removeprefix("www.")
    query = parse_qs(parsed.query)

    if "play.google.com" in host and query.get("id"):
        return f"google:{query['id'][0].lower()}"

    apple_id = re.search(r"/id(\d+)", parsed.path, flags=re.IGNORECASE)
    if ("apple.com" in host or "itunes" in host) and apple_id:
        return f"apple:{apple_id.group(1)}"

    path = parsed.path.rstrip("/").lower()
    return f"{host}{path}" if host and path else ""


def row_link_tokens(row: dict[str, object]) -> set[str]:
    return {
        token
        for _, url in download_links(row.get("7", ""))
        if (token := canonical_link_token(url))
    }


def csv_link_tokens(row: dict[str, str]) -> set[str]:
    values = f"{row.get('Download links', '')} | {row.get('Téléchargements', '')}"
    urls = re.findall(r"https?://[^\s|]+", values)
    return {
        token for url in urls if (token := canonical_link_token(url.rstrip(".,;)")))
    }


def load_feed(source: str) -> list[dict[str, object]]:
    if source.startswith(("http://", "https://")):
        response = requests.get(source, timeout=60)
        response.raise_for_status()
        payload = response.json()
    else:
        with Path(source).open(encoding="utf-8-sig") as handle:
            import json

            payload = json.load(handle)

    rows = payload.get("aaData") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"Feed {source!r} does not contain a non-empty aaData list")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"Feed {source!r} contains an invalid row")
    return rows


def pair_bilingual_rows(
    english_rows: list[dict[str, object]], french_rows: list[dict[str, object]]
) -> list[tuple[dict[str, object] | None, dict[str, object] | None]]:
    """Pair language records by common app-store identifiers, not row order."""
    available_french = set(range(len(french_rows)))
    french_tokens = [row_link_tokens(row) for row in french_rows]
    pairs: list[tuple[dict[str, object] | None, dict[str, object] | None]] = []

    for index, english in enumerate(english_rows):
        tokens = row_link_tokens(english)
        candidates = [
            (len(tokens & french_tokens[french_index]), french_index)
            for french_index in available_french
        ]
        score, match = max(candidates, default=(0, -1))

        if score == 0:
            english_id = str(english.get("DT_RowId", ""))
            same_id = [
                french_index
                for french_index in available_french
                if english_id
                and english_id == str(french_rows[french_index].get("DT_RowId", ""))
            ]
            match = same_id[0] if len(same_id) == 1 else -1

        if match >= 0:
            available_french.remove(match)
            pairs.append((english, french_rows[match]))
        else:
            pairs.append((english, None))

    pairs.extend((None, french_rows[index]) for index in sorted(available_french))
    return pairs


def merged_row(
    english: dict[str, object] | None, french: dict[str, object] | None
) -> dict[str, str]:
    english = english or {}
    french = french or {}
    date_value = text_from_html(english.get("1", "")) or text_from_html(
        french.get("1", "")
    )
    platforms = platforms_from_html(english.get("2", "")) or platforms_from_html(
        french.get("2", "")
    )
    icon = icon_from_html(english.get("0", "")) or icon_from_html(french.get("0", ""))

    return {
        "Name": app_name(english.get("0", "")) if english else "",
        "Nom": app_name(french.get("0", "")) if french else "",
        "Publication date": date_value,
        "Platforms": platforms,
        "Short description": text_from_html(english.get("3", "")),
        "Courte description": text_from_html(french.get("3", "")),
        "Long description": text_from_html(english.get("4", "")),
        "Longue description": text_from_html(french.get("4", "")),
        "Published by": publisher_from_html(english.get("5", ""), "en"),
        "Publié par": publisher_from_html(french.get("5", ""), "fr"),
        "Department": text_from_html(english.get("6", "")),
        "Ministère": text_from_html(french.get("6", "")),
        "Download links": format_download_links(english.get("7", "")),
        "Téléchargements": format_download_links(french.get("7", "")),
        "icon": icon,
    }


def normalized_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def same_app(left: dict[str, str], right: dict[str, str]) -> bool:
    left_tokens = csv_link_tokens(left)
    right_tokens = csv_link_tokens(right)
    if left_tokens and right_tokens and left_tokens & right_tokens:
        return True

    return any(
        normalized_name(left.get(column, ""))
        and normalized_name(left.get(column, ""))
        == normalized_name(right.get(column, ""))
        for column in ("Name", "Nom")
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def update_outputs(
    current_path: Path,
    deleted_path: Path,
    current_rows: list[dict[str, str]],
    removal_date: str,
) -> tuple[int, int]:
    previous_rows = read_csv(current_path)
    deleted_rows = read_csv(deleted_path)
    newly_deleted = [
        {**row, "Removal detected date": removal_date}
        for row in previous_rows
        if not any(same_app(row, current) for current in current_rows)
    ]

    # Prevent a same-day rerun from duplicating an identical deletion event.
    existing_events = {
        tuple(row.get(column, "") for column in DELETED_COLUMNS) for row in deleted_rows
    }
    deleted_rows.extend(
        row
        for row in newly_deleted
        if tuple(row.get(column, "") for column in DELETED_COLUMNS)
        not in existing_events
    )

    write_csv(current_path, COLUMNS, current_rows)
    write_csv(deleted_path, DELETED_COLUMNS, deleted_rows)
    return len(current_rows), len(newly_deleted)


def daily_department_counts(
    current_rows: list[dict[str, str]], snapshot_date: str
) -> list[dict[str, str | int]]:
    """Count unique apps and platform availability for each department."""
    groups: dict[tuple[str, str], dict[str, str | int]] = {}
    for app in current_rows:
        key = (app.get("Department", ""), app.get("Ministère", ""))
        if key not in groups:
            groups[key] = {
                "date": snapshot_date,
                "Department": key[0],
                "Ministère": key[1],
                "unique_app_count": 0,
                "iOS_count": 0,
                "android_count": 0,
                "blackberry_count": 0,
                "amazon_count": 0,
            }

        group = groups[key]
        group["unique_app_count"] = int(group["unique_app_count"]) + 1
        platforms = {
            platform.strip().casefold()
            for platform in app.get("Platforms", "").split(";")
            if platform.strip()
        }
        for platform, column in PLATFORM_COUNTS.items():
            if platform in platforms:
                group[column] = int(group[column]) + 1

    return sorted(groups.values(), key=lambda row: str(row["Department"]).casefold())


def update_daily_count_history(
    path: Path, snapshot_rows: list[dict[str, str | int]], snapshot_date: str
) -> None:
    """Append a daily snapshot, replacing that date if the workflow is rerun."""
    history = [row for row in read_csv(path) if row.get("date") != snapshot_date]
    history.extend(snapshot_rows)
    history.sort(
        key=lambda row: (
            str(row.get("date", "")),
            str(row.get("Department", "")).casefold(),
        )
    )
    write_csv(path, COUNT_COLUMNS, history)


def sankey_csv_line(source: str, target: str, count: int) -> str:
    """Create one correctly quoted Mermaid Sankey CSV line."""
    from io import StringIO

    buffer = StringIO()
    csv.writer(buffer, lineterminator="").writerow([source, target, count])
    return f"  {buffer.getvalue()}"


def render_mobile_apps_sankey(
    snapshot_rows: list[dict[str, str | int]], snapshot_date: str
) -> str:
    lines = [
        SANKEY_START,
        "### Mobile apps by department and platform",
        "",
        (
            f"Snapshot date: **{snapshot_date}**. "
            "Department labels show unique apps. Because one app can support several "
            "platforms, its outgoing platform counts may sum above that unique total."
        ),
        "",
        "[View the daily department and platform count history](mobile-apps/mobile_app_counts.csv).",
        "",
        "```mermaid",
        "sankey-beta",
    ]
    for row in snapshot_rows:
        unique_count = int(row["unique_app_count"])
        noun = "app" if unique_count == 1 else "apps"
        source = f"{row['Department']} ({unique_count} unique {noun})"
        for platform, column in (
            ("iOS", "iOS_count"),
            ("Android", "android_count"),
            ("BlackBerry", "blackberry_count"),
            ("Amazon", "amazon_count"),
        ):
            count = int(row[column])
            if count:
                lines.append(sankey_csv_line(source, platform, count))
    lines.extend(["```", SANKEY_END, ""])
    return "\n".join(lines)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def update_readme_sankey(readme_path: Path, sankey: str) -> None:
    readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    marker_pattern = re.compile(
        rf"{re.escape(SANKEY_START)}.*?{re.escape(SANKEY_END)}\n?",
        flags=re.DOTALL,
    )
    if marker_pattern.search(readme):
        updated = marker_pattern.sub(sankey, readme, count=1)
    else:
        insertion_point = "# Social Media Platform Overview"
        if insertion_point in readme:
            updated = readme.replace(insertion_point, f"{sankey}\n{insertion_point}", 1)
        else:
            updated = f"{readme.rstrip()}\n\n{sankey}".lstrip()
    write_text(readme_path, updated)


def parse_args() -> argparse.Namespace:
    directory = Path(__file__).resolve().parent
    repository = directory.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--english-json", default=ENGLISH_URL)
    parser.add_argument("--french-json", default=FRENCH_URL)
    parser.add_argument("--output", type=Path, default=directory / "mobile_apps.csv")
    parser.add_argument(
        "--deleted-output", type=Path, default=directory / "deleted_mobile_apps.csv"
    )
    parser.add_argument(
        "--icon-directory",
        type=Path,
        help="Directory for decoded icons (defaults to an icons folder beside --output)",
    )
    parser.add_argument("--removal-date", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--snapshot-date")
    parser.add_argument(
        "--counts-output",
        type=Path,
        default=directory / "mobile_app_counts.csv",
    )
    parser.add_argument(
        "--sankey-output",
        type=Path,
        default=directory / "mobile_apps_sankey.md",
    )
    parser.add_argument("--readme", type=Path, default=repository / "README.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    english_rows = load_feed(args.english_json)
    french_rows = load_feed(args.french_json)
    rows = [
        merged_row(english, french)
        for english, french in pair_bilingual_rows(english_rows, french_rows)
    ]
    icon_directory = args.icon_directory or args.output.parent / "icons"
    for row in rows:
        row["icon"] = materialize_icon(row["icon"], icon_directory, args.output.parent)
    current_count, deleted_count = update_outputs(
        args.output, args.deleted_output, rows, args.removal_date
    )
    snapshot_date = args.snapshot_date or args.removal_date
    snapshot_rows = daily_department_counts(rows, snapshot_date)
    update_daily_count_history(args.counts_output, snapshot_rows, snapshot_date)
    sankey = render_mobile_apps_sankey(snapshot_rows, snapshot_date)
    write_text(args.sankey_output, sankey)
    update_readme_sankey(args.readme, sankey)
    print(
        f"Wrote {current_count} current apps to {args.output}; "
        f"recorded {deleted_count} newly removed apps in {args.deleted_output}; "
        f"wrote {len(snapshot_rows)} department counts to {args.counts_output}."
    )


if __name__ == "__main__":
    main()
