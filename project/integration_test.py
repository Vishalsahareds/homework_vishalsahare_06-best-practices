import os
import pandas as pd

# Set up Localstack S3 endpoint
S3_ENDPOINT_URL = "http://localhost:4566"
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

def get_input_path(year, month):
    default_input_pattern = 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def read_data(file_path):
    df = pd.read_parquet(file_path, storage_options=options)
    return df

def save_data(df, file_path):
    df.to_parquet(file_path, engine='pyarrow', compression=None, index=False, storage_options=options)

def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Create a dataframe (pretend this is data for January 2023)
    # data = {
    #     'column1': [1, 2, 3],
    #     'column2': ['a', 'b', 'c'],
    #     'predicted_duration': [13.08, 36.28, 69.28]
    # }

    data = {
        'column2': ['b'],
        'predicted_duration': [36.28]
    }

    df_input = pd.DataFrame(data)

    # Save dataframe to S3
    save_data(df_input, input_file)

    print("Dataframe saved to S3.")

    # Run the batch.py script
    os.system(f"python batch.py {year} {month}")

    # Verify the saved data
    df_output = pd.read_parquet(output_file, storage_options=options)
    print("Data read from S3:")
    print(df_output)

    # Calculate the sum of predicted durations
    sum_predicted_durations = df_output['predicted_duration'].sum()
    print(f"Sum of predicted durations: {sum_predicted_durations}")

if __name__ == "__main__":
    main(2023, 1)
