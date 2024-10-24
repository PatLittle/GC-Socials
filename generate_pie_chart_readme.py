import pandas as pd

# Load the CSV data
df = pd.read_csv('sm.csv')

# Calculate the frequency of each platform
platform_counts = df['Platform'].value_counts()

# Generate the Mermaid.js pie chart syntax
pie_chart = "```mermaid\npie showData title Platform Distribution\n"
for platform, count in platform_counts.items():
    pie_chart += f"    \"{platform}\": {count}\n"
pie_chart += "```"

# Write the pie chart to the README file
with open('README.md', 'w') as readme_file:
    readme_file.write("# Social Media Platform Distribution\n\n")
    readme_file.write(pie_chart)

print("Mermaid.js pie chart has been added to the README.md file.")
