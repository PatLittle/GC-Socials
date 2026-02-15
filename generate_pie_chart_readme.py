import pandas as pd

# Load the CSV data
df = pd.read_csv('sm.csv')

# Calculate the frequency of each platform
platform_counts = df['Platform'].value_counts()

# Generate the Mermaid.js pie chart syntax for Platform
platform_pie_chart = "```mermaid\npie showData title Platform Distribution\n"
for platform, count in platform_counts.items():
    platform_pie_chart += f"    \"{platform}\": {count}\n"
platform_pie_chart += "```"

# Calculate the frequency of each language
language_counts = df['Language'].value_counts()

# Generate the Mermaid.js pie chart syntax for Language
language_pie_chart = "```mermaid\npie showData title Language Distribution\n"
for language, count in language_counts.items():
    language_pie_chart += f"    \"{language}\": {count}\n"
language_pie_chart += "```"

# Calculate the frequency of each department where Language is English (top 20)
department_counts_english = df[df['Language'] == 'English']['Department'].value_counts().nlargest(20)

# Generate the Mermaid.js pie chart syntax for Department (English)
department_pie_chart_english = "```mermaid\npie showData title Department Count (English Only - Top 20)\n"
for department, count in department_counts_english.items():
    department_pie_chart_english += f"    \"{department}\": {count}\n"
department_pie_chart_english += "```"

# Calculate the frequency of each department where Language is Français (top 20)
department_counts_french = df[df['Language'] == 'Français']['Department'].value_counts().nlargest(20)

# Generate the Mermaid.js pie chart syntax for Department (Français)
department_pie_chart_french = "```mermaid\npie showData title Department Count (Français Only - Top 20)\n"
for department, count in department_counts_french.items():
    department_pie_chart_french += f"    \"{department}\": {count}\n"
department_pie_chart_french += "```"

# Calculate the frequency of each department where Language is Bilingual or Bilingue (top 20)
department_counts_bilingual = df[df['Language'].isin(['Bilingual', 'Bilingue'])]['Department'].value_counts().nlargest(20)

# Generate the Mermaid.js pie chart syntax for Department (Bilingual)
department_pie_chart_bilingual = "```mermaid\npie showData title Department Count (Bilingual Only - Top 20)\n"
for department, count in department_counts_bilingual.items():
    department_pie_chart_bilingual += f"    \"{department}\": {count}\n"
department_pie_chart_bilingual += "```"

with open('readme_static.md', 'r') as static_file:
    static_content = static_file.read()

with open('sankey_diagram.md', 'r') as sankey_file:
    sankey = sankey_file.read()

with open('recent_changes_snippet.md', 'r') as recent_changes_file:
    recent_changes = recent_changes_file.read()

with open('README.md', 'w') as readme_file:
    readme_file.write(static_content + "\n\n")
    readme_file.write("# Social Media Platform Overview\n\n")
    readme_file.write(sankey + "\n\n")
    readme_file.write(recent_changes + "\n\n")
    readme_file.write("# Social Media Platform Distribution\n\n")
    readme_file.write(platform_pie_chart + "\n\n")
    readme_file.write("# Language Distribution\n\n")
    readme_file.write(language_pie_chart + "\n\n")
    readme_file.write("# Department Count (English Only - Top 20)\n\n")
    readme_file.write(department_pie_chart_english + "\n\n")
    readme_file.write("# Department Count (Français Only - Top 20)\n\n")
    readme_file.write(department_pie_chart_french + "\n\n")
    readme_file.write("# Department Count (Bilingual Only - Top 20)\n\n")
    readme_file.write(department_pie_chart_bilingual)

print("Static content, Mermaid.js pie charts have been added to the README.md file.")
