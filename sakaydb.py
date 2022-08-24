import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class SakayDBError(ValueError):
    pass

class SakayDB():

    def __init__(self, data_dir):
        """Initializes by taking path to the data and reading the necessary csvs for SakayDB."""
        self.data_dir = data_dir

    def add_trip(self, driver, pickup_datetime, dropoff_datetime, passenger_count,
             pickup_loc_name, dropoff_loc_name, trip_distance, fare_amount):
        #Step 0: Import dfs question 1: should the file always exist regardless of size?
        trips = pd.read_csv(data_dir + 'trips_test.csv')
        drivers = pd.read_csv(data_dir + 'drivers_test.csv')
        locations = pd.read_csv(data_dir + 'locations.csv')

        #Step 1: Convert input to trips row (function input -> expected entry)

        #Step 1a: Get or create driver_id
        driver.split(', ')
        last_name = driver.split(', ')[0]
        given_name = driver.split(', ')[1]

        if drivers[(drivers.given_name.str.lower() == given_name.lower()) 
                   & (drivers.last_name.str.lower() == last_name.lower())].shape[0] > 0:
            driver_id = (drivers[(drivers.given_name.str.lower() == given_name.lower()) 
                                 & (drivers.last_name.str.lower() == last_name.lower())]
                         ['driver_id'].values[0])
        else:
            if drivers.shape[0] == 0: #gotta check with this
                driver_id = 1
            else:
                driver_id = drivers['driver_id'].iloc[-1] + 1

            drivers.loc[len(drivers.index)] = [drivers['driver_id'].iloc[-1] + 1,
                                               given_name,
                                               last_name]
        #Step 1b: Get pickup and dropoff id
        pickup_loc_id = locations[locations.loc_name == pickup_loc_name]['location_id'].values[0]
        dropoff_loc_id = locations[locations.loc_name == dropoff_loc_name]['location_id'].values[0]

        #Step 2: Insert row
        #Step 2a: Check if row exists -> out: SakayDBError
        #Step 2b: If row not exist, append at end of file

        print(driver_id)
        print(pickup_loc_id, dropoff_loc_id)
        print(drivers)
        #to_csv section
        #trips.to_csv('trips.csv', index=False)
        #drivers.to_csv('drivers.csv', index=False)
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

    def generate_odmatrix(self):
        pass