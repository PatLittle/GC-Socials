# Government of Canada mobile apps

This directory tracks the bilingual Government of Canada mobile-app inventory.

- `mobile_apps.csv` is the current inventory, rebuilt from the English and French Canada.ca JSON feeds. Its `icon` values are relative paths into `icons/`.
- `deleted_mobile_apps.csv` is an append-only history of apps that disappear from the current feeds. `Removal detected date` is the workflow date on which the removal was first observed.
- `mobile_app_counts.csv` is the daily history of unique app counts and iOS, Android, BlackBerry, and Amazon availability by bilingual department name. A multi-platform app contributes once to `unique_app_count` and once to each applicable platform column.
- `mobile_apps_sankey.md` is the generated Mermaid source embedded in the repository-level README.
- `icons/` contains decoded source icons, named by content hash. Icons are retained when an app is removed so deletion-history paths remain valid.
- `update_mobile_apps.py` strips the source HTML, pairs the two languages using shared app-store identifiers, writes the merged inventory and icons, and compares it with the preceding CSV to detect removals.

The source feeds are:

- <https://www.canada.ca/content/dam/canada/json/mob-en.json>
- <https://www.canada.ca/content/dam/canada/json/mob-fr.json>

Run locally from the repository root:

```bash
python -m pip install -r mobile-apps/requirements.txt
python mobile-apps/update_mobile_apps.py
```

The scheduled workflow downloads both feeds before invoking the script, so a failed or empty source cannot silently replace the inventory.
