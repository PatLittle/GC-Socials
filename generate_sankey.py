import pandas as pd

# Load the CSV file
df = pd.read_csv('platform_counts.csv')

# Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# Drop rows with NaT or NaN in 'Date' or 'Language' columns
df_cleaned = df.dropna(subset=['Date', 'Language'])

# Normalize platform names
df_cleaned['Platform'] = df_cleaned['Platform'].replace({
    'Linkedin': 'LinkedIn',
    'Intagram': 'Instagram',
    'Youtube': 'YouTube',
    'x': 'X',
    'snapchat': 'Snapchat'
})

# Get the most recent date in the cleaned dataset
most_recent_date = df_cleaned['Date'].max()

# Filter the dataframe to only include rows from the most recent date
most_recent_df = df_cleaned[df_cleaned['Date'] == most_recent_date]

# Normalize Bilingual and Bilingue to a single label
most_recent_df['Language'] = most_recent_df['Language'].replace({'Bilingual': 'Bilingual + Bilingue', 'Bilingue': 'Bilingual + Bilingue'})

# Group by Language and Platform, and sum the counts
grouped_df = most_recent_df.groupby(['Language', 'Platform'])['Count'].sum().reset_index()

# Create the Sankey diagram input based on the grouped data
sankey_lines = ['sankey-beta']

# Add language to platform connections
for _, row in grouped_df.iterrows():
    sankey_lines.append(f"  {row['Language']},{row['Platform']},{row['Count']}")

# Write the Sankey diagram to a file
with open('sankey_diagram.txt', 'w') as f:
    f.write("\n".join(sankey_lines))
