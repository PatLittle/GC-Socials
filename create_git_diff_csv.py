import pandas as pd

# Read git diff information
with open('git_diff.txt', 'r') as f:
    diff_data = f.readlines()

# Convert diff data to DataFrame
diff_df = pd.DataFrame({'diff': diff_data})
diff_df.to_csv('git_diff.csv', index=False)

print("CSV file 'git_diff.csv' has been created successfully.")
