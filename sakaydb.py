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
        """
        Merges trips.csv, drivers.csv, locations.csv 
        and returns a table containing all the columns from each.

        Returns
        -------
        """
        if (not os.path.isfile(os.path.join(self.data_dir, 'trips.csv'))
            or not os.path.isfile(os.path.join(self.data_dir, 'drivers.csv'))
            or not os.path.isfile(os.path.join(self.data_dir, 'locations.csv'))):
            df = pd.DataFrame(columns=['dropoff_loc_name', 'passenger_count', 'trip_distance',
                                       'dropoff_datetime', 'fare_amount', 'driver_lastname',
                                       'pickup_loc_name', 'driver_givenname', 'pickup_datetime'])
            return df
        else:
            trips = pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
            drivers = pd.read_csv(os.path.join(self.data_dir, 'drivers.csv'))
            pu_locations = pd.read_csv(os.path.join(self.data_dir, 'locations.csv'))
            do_locations = pd.read_csv(os.path.join(self.data_dir, 'locations.csv'))
            pu_locations.rename(columns={'location_id': 'pickup_loc_id'}, inplace=True)
            do_locations.rename(columns={'location_id': 'dropoff_loc_id'}, inplace=True)

            df = pd.merge(trips, drivers, on='driver_id')
            df = pd.merge(df, pu_locations, on='pickup_loc_id').rename(columns={'loc_name': 'pickup_loc_name'})
            df = pd.merge(df, do_locations, on='dropoff_loc_id').rename(columns={'loc_name': 'dropoff_loc_name'})

            df.rename(columns={
                'last_name': 'driver_lastname',
                'given_name': 'driver_givenname'
            }, inplace=True)

            df.sort_values('trip_id', inplace=True)
            df = df[['dropoff_loc_name', 'passenger_count', 'trip_distance',
                     'dropoff_datetime', 'fare_amount', 'driver_lastname',
                     'pickup_loc_name', 'driver_givenname', 'pickup_datetime']]
            return df

    def generate_statistics(self):
        pass

    def plot_statistics(self):
        pass

    def generate_odmatrix(self):
        pass
