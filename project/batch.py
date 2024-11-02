import pandas as pd

def read_data(file_path):
    df = pd.read_parquet(file_path)
    return df

def prepare_data(df):
    # Ensure the correct data types
    df['PULocationID'] = df['PULocationID'].astype('Int64')
    df['DOLocationID'] = df['DOLocationID'].astype('Int64')
    # Apply transformations to the dataframe
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    df = df.dropna(subset=['PULocationID', 'DOLocationID'])
    return df

if __name__ == "__main__":
    file_path = 'input_yellow_tripdata_2023-03.parquet'
    df = read_data(file_path)
    transformed_df = prepare_data(df)
    print(transformed_df)
