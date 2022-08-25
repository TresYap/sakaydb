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
        trips = pd.read_csv(self.data_dir + 'trips.csv')
        drivers = pd.read_csv(self.data_dir + 'drivers.csv')
        locations = pd.read_csv(self.data_dir + 'locations.csv')

        #Step 1: Convert input to trips row (function input -> expected entry)

        #Step 1a: Get or create driver_id
        driver.split(', ')
        last_name = driver.split(', ')[0]
        given_name = driver.split(', ')[1]

        if drivers.shape[0] == 0:
            driver_id = 1
        elif drivers[(drivers.given_name.str.lower() == given_name.lower()) 
                   & (drivers.last_name.str.lower() == last_name.lower())].shape[0] > 0:
            driver_id = (drivers[(drivers.given_name.str.lower() == given_name.lower()) 
                                 & (drivers.last_name.str.lower() == last_name.lower())]
                         ['driver_id'].values[0])
        else:
            driver_id = drivers['driver_id'].iloc[-1] + 1
            new_row = pd.DataFrame({
                'driver_id': [driver_id],
                'given_name': [given_name],
                'last_name': [last_name]
            })
            drivers = pd.concat([drivers, new_row], ignore_index=True)

        #Step 1b: Get or create pickup and dropoff id
        if locations.shape[0] == 0:
            pickup_loc_id = 1
            dropoff_loc_id = 2
        else:
            if pickup_loc_name in locations.loc_name.values.tolist():
                pickup_loc_id = (locations[locations.loc_name == pickup_loc_name]['location_id']
                                 .values[0])
            else:
                pickup_loc_id = locations['location_id'].iloc[-1] + 1
                new_row = pd.DataFrame({
                'location_id': [pickup_loc_id],
                'loc_name': [pickup_loc_name],
                })
                locations = pd.concat([locations, new_row], ignore_index=True)

            if dropoff_loc_name in locations.loc_name.values.tolist():
                dropoff_loc_id = (locations[locations.loc_name == dropoff_loc_name]['location_id']
                                  .values[0])
            else:
                dropoff_loc_id = locations['location_id'].iloc[-1] + 1
                new_row = pd.DataFrame({
                'location_id': [dropoff_loc_id],
                'loc_name': [dropoff_loc_name],
                })
                locations = pd.concat([locations, new_row], ignore_index=True)

        #Step 2: Insert row
        #Step 2a: Check if row exists -> out: SakayDBError
        # driver (case-insensitive), pickup_datetime, dropoff_datetime, passenger_count,
        # pickup_loc_name, dropoff_loc_name, trip_distance and fare_amount
        #TODO: check if timestamp matching necessary or if string is enough

        row = {
            'driver_id': driver_id,
            'pickup_datetime': pickup_datetime,
            'dropoff_datetime': dropoff_datetime,
            'passenger_count': passenger_count,
            'pickup_loc_id': pickup_loc_id,
            'dropoff_loc_id': dropoff_loc_id,
            'trip_distance': trip_distance,
            'fare_amount': fare_amount
        }
        if row in trips.loc[:, trips.columns != 'trip_id'].to_dict(orient='records'):
            raise SakayDBError
        else:
            row['trip_id'] = trips['trip_id'].iloc[-1] + 1
            trips = pd.concat([trips, pd.DataFrame.from_dict([row])], ignore_index=True)

        #Step 2b: If row not exist, append at end of file
        display(drivers)
        display(locations.tail())
        display(trips)
        #to_csv section
        trips.to_csv(self.data_dir + 'trips.csv', index=False)
        drivers.to_csv(self.data_dir + 'drivers.csv', index=False)
        locations.to_csv(self.data_dir + 'locations.csv', index=False)

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