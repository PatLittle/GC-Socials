import pandas as pd

# Load the CSV data
df = pd.read_csv('sm.csv')

# Calculate the frequency of each platform
platform_counts = df['Platform'].value_counts()

# Generate the Mermaid.js pie chart syntax for Platform
platform_pie_chart = "```mermaid\npie title Platform Distribution\n"
for platform, count in platform_counts.items():
    platform_pie_chart += f"    \"{platform}\": {count}\n"
platform_pie_chart += "```"

# Calculate the frequency of each language
language_counts = df['Language'].value_counts()

# Generate the Mermaid.js pie chart syntax for Language
language_pie_chart = "```mermaid\npie title Language Distribution\n"
for language, count in language_counts.items():
    language_pie_chart += f"    \"{language}\": {count}\n"
language_pie_chart += "```"

# Read static markdown content from readme_static.md
with open('readme_static.md', 'r') as static_file:
    static_content = static_file.read()

# Append the static content and pie charts to the README file
with open('README.md', 'w') as readme_file:
    readme_file.write(static_content + "\n\n")
    readme_file.write("# Social Media Platform Distribution\n\n")
    readme_file.write(platform_pie_chart + "\n\n")
    readme_file.write("# Language Distribution\n\n")
    readme_file.write(language_pie_chart)

print("Static content and Mermaid.js pie charts have been added to the README.md file.")
