# Databricks notebook source
import pytest
from batch import main, read_data

def test_read_data():
    assert read_data('some-file-location',['','']) is not None

def test_main():
    # # Example test case for the main function
    # year = 2023
    # month = 3
    # result = main(year, month)
    # assert result is None  # Adjust this based on the actual return value of main
    assert main(2021, 3) is None