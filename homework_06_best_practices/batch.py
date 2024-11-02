

import os
import pandas as pd

S3_ENDPOINT_URL = "http://localhost:4566"
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

def read_data(file_path):
    df = pd.read_parquet(file_path, storage_options=options)
    return df

def save_data(df, file_path):
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }
    df.to_parquet(file_path, engine='pyarrow', compression=None, index=False, storage_options=options)

if __name__ == "__main__":
    year = 2023
    month = 1
    input_file = f"s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    output_file = f"s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

    df = read_data(input_file)
    # Perform some operations on df if needed
    save_data(df, output_file)

