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
    # export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_path(year, month):
    #OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    #output_pattern = OUTPUT_FILE_PATTERN
    return output_pattern.format(year=year, month=month)

def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Create a dataframe (pretend this is data for January 2023)
    data = {
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    }
    df_input = pd.DataFrame(data)

    # Save dataframe to S3
    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

    print("Dataframe saved to S3.")

if __name__ == "__main__":
    main(2023, 3)
