import pandas as pd

# Read the HTML file
data = pd.read_html('./table_data.html')
# Print the first 5 rows of the first table

print(data[0].head())
# 