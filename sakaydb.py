import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class SakayDBError(ValueError):
    pass

class SakayDB():

    def __init__(self, data_dir):
        """Initializes by taking path to the data and reading the necessary csvs for SakayDB."""
        self.data_dir = data_dir

    def add_trip(self):
        pass

    def add_trips(self):
        pass

    def delete_trip(self):
        pass

    def search_trips(self):
        pass

    def export_data(self):
        pass

    def generate_statistics(self):
        pass

    def plot_statistics(self):
        pass

    def generate_odmatrix(self, date_range=(None, None)):
        # Check if date range tuple items are greater than 2
        if len(date_range) > 2:
            raise SakayDBError()
        else:
            pass

        # Set date format
        format = '%H:%M:%S,%d-%m-%Y'

        # Check if date format is correct
        try:
            if (date_range[0] is None) & (date_range[1] is not None):
                datetime.strptime(date_range[1], format)
            elif (date_range[0] is not None) & (date_range[1] is None):
                datetime.strptime(date_range[0], format)
            elif (date_range[0] is not None) & (date_range[1] is not None):
                datetime.strptime(date_range[0], format)
                datetime.strptime(date_range[1], format)
            else:
                pass
        except ValueError:
            raise SakayDBError('Invalid date range.')

        # Get file path of trips and locations csv files
        trips_path = self.data_dir + '/trips.csv'
        locations_path = self.data_dir + '/locations.csv'

        # Check if trips.csv exists in the directory
        try:
            f = open(trips_path)
        except IOError:
            return pd.DataFrame({'A': []})

        # Read trips.csv file and store to a df
        trips = pd.read_csv(trips_path)
        locations = pd.read_csv(locations_path)

        # Convert str date to datetime
        format = '%H:%M:%S,%d-%m-%Y'
        trips['pickup_datetime'] = pd.to_datetime(trips['pickup_datetime'],
                                                  format=format)
        trips['dropoff_datetime'] = pd.to_datetime(trips['dropoff_datetime'],
                                                   format=format)

        # Convert date_range to datetime then to set interval
        min_date = pd.to_datetime(date_range[0], format=format)
        max_date = pd.to_datetime(date_range[1], format=format)

        # Filter rows that are not within date_range
        if (max_date is None) & (min_date is not None):
            trips = trips.loc[trips['pickup_datetime'] >= min_date]
        elif (min_date is None) & (max_date is not None):
            trips = trips.loc[trips['pickup_datetime'] <= min_date]
        elif (min_date is None) & (max_date is None):
            trips = trips
        else:
            trips = trips.loc[(trips['pickup_datetime'] >= min_date) &
                              (trips['dropoff_datetime'] <= max_date)]

        # Add column to count the unique dropoff-pickup combinations
        trips['unique_droppick'] = 1.0

        # Replace pickup loc id with pickup loc name
        loc_df = locations.rename(columns={'location_id': 'pickup_loc_id'})
        trips = (trips.merge(loc_df, how='left', on='pickup_loc_id')
                 .rename(columns={'loc_name': 'pickup_loc_name'}))

        # Replace dropoff loc id with dropoff up loc name
        loc_df = locations.rename(columns={'location_id': 'dropoff_loc_id'})
        trips = (trips.merge(loc_df, how='left', on='dropoff_loc_id')
                 .rename(columns={'loc_name': 'dropoff_loc_name'}))

        # Drop loc_id columns
        trips.drop(['pickup_loc_id', 'dropoff_loc_id'], axis=1, inplace=True)

        # Get the number of daily trips for each
        # unique dropoff-pickup location combinations
        trips = (trips.groupby(['dropoff_loc_name', 'pickup_loc_name',
                                pd.Grouper(key='pickup_datetime',
                                           freq='1D')])
                 ['unique_droppick'].sum().reset_index())

        # Get the average daily trips for each dropoff-pickup combinations
        trips = (trips.groupby(['dropoff_loc_name', 'pickup_loc_name'])
                 ['unique_droppick'].mean().reset_index())

        # Create the matrix
        final_df = (trips.pivot(index='dropoff_loc_name',
                                columns='pickup_loc_name',
                                values='unique_droppick').fillna(0))

        return final_df
    
