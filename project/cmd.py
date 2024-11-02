import pandas as pd

# Read the Parquet file into a pandas DataFrame
df = pd.read_parquet('input/input_yellow_tripdata_2023-03.parquet')

# Display the top 10 rows in the DataFrame
print(df.head(10))
print(df.info())