import unittest
import pandas as pd
from datetime import datetime
from project.batch import prepare_data

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

class TestBatch(unittest.TestCase):

    def test_prepare_data(self):
        data = [
            (None, None, dt(1, 1), dt(1, 10)),
            (1, 1, dt(1, 2), dt(1, 10)),
            (1, None, dt(1, 2, 0), dt(1, 2, 59)),
            (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
        ]
        columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
        df = pd.DataFrame(data, columns=columns)

        expected_data = [
            (1, 1, dt(1, 2), dt(1, 10), 8.0),
            (3, 4, dt(1, 2, 0), dt(2, 2, 1), 60.016666666666666),
        ]
        expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration']
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        # Ensure the correct data types in the expected DataFrame
        expected_df['PULocationID'] = expected_df['PULocationID'].astype('Int64')
        expected_df['DOLocationID'] = expected_df['DOLocationID'].astype('Int64')

        result_df = prepare_data(df)

        pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

if __name__ == '__main__':
    unittest.main()
