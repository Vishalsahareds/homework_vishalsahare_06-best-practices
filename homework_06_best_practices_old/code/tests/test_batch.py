# Databricks notebook source
# import pytest
# from batch import main, read_data

# def test_read_data():
#     assert read_data('some-file-location',['','']) is not None

# def test_main():
#     # # Example test case for the main function
#     # year = 2023
#     # month = 3
#     # result = main(year, month)
#     # assert result is None  # Adjust this based on the actual return value of main
#     assert main(2021, 3) is None


import pytest
from batch import main, read_data

def test_read_data():
    # Provide a valid file location and categorical columns for testing
    df = read_data('tests/yellow_tripdata_2023-03.parquet', ['PULocationID', 'DOLocationID'])
    assert df is not None
    assert 'duration' in df.columns
    assert 'PULocationID' in df.columns
    assert 'DOLocationID' in df.columns

def test_main():
    # Example test case for the main function
    year = 2023
    month = 3
    result = main(year, month)
    assert result is None  # Adjust this based on the actual return value of main