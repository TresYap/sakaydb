import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os


class SakayDBError(ValueError):

     def __init__(self, message="SakayDBError"):
        self.message = message
        super().__init__(self.message)


class SakayDB():

    def __init__(self, data_dir):
        """Initializes by taking path to the data
        and reading the necessary csvs for SakayDB."""
        self.data_dir = data_dir

    def add_trip(self, driver, pickup_datetime, dropoff_datetime,
                 passenger_count, pickup_loc_name, dropoff_loc_name,
                 trip_distance, fare_amount):
        try:
            trips = pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
        except FileNotFoundError:
            trips = pd.DataFrame(columns=['trip_id',
                                          'driver_id',
                                          'pickup_datetime',
                                          'dropoff_datetime',
                                          'passenger_count',
                                          'pickup_loc_id',
                                          'dropoff_loc_id',
                                          'trip_distance',
                                          'fare_amount'])
        try:
            drivers = pd.read_csv(os.path.join(self.data_dir, 'drivers.csv'))
        except FileNotFoundError:
            drivers = pd.DataFrame(columns=['driver_id',
                                            'given_name',
                                            'last_name'])

        try:
            locations = pd.read_csv(os.path.join(self.data_dir,
                                                 'locations.csv'))
        except FileNotFoundError:
            locations = pd.DataFrame(columns=['location_id',
                                              'loc_name'])

        names = driver.strip().split(', ')
        last_name = names[0]
        given_name = names[1]
        pickup_loc_name = pickup_loc_name.strip()
        dropoff_loc_name = dropoff_loc_name.strip()

        if drivers.shape[0] == 0:
            driver_id = 1
            new_row = pd.DataFrame({
                'driver_id': [driver_id],
                'given_name': [given_name],
                'last_name': [last_name]
            })
            drivers = pd.concat([drivers, new_row], ignore_index=True)
        elif drivers[(drivers.given_name.str.lower() == given_name.lower())
                     & (drivers.last_name.str.lower() == last_name.lower())
                     ].shape[0] > 0:
            driver_id = (drivers[(drivers.given_name.str.lower()
                                  == given_name.lower()) &
                                 (drivers.last_name.str.lower()
                                  == last_name.lower())]
                         ['driver_id'].values[0])
        else:
            driver_id = drivers['driver_id'].iloc[-1] + 1
            new_row = pd.DataFrame({
                'driver_id': [driver_id],
                'given_name': [given_name],
                'last_name': [last_name]
            })
            drivers = pd.concat([drivers, new_row], ignore_index=True)

        if locations.shape[0] == 0:
            pickup_loc_id = 1
            dropoff_loc_id = 2
        else:
            if pickup_loc_name in locations.loc_name.values.tolist():
                pickup_loc_id = (locations[locations.loc_name
                                           == pickup_loc_name]
                                 ['location_id']
                                 .values[0])
            else:
                pickup_loc_id = locations['location_id'].iloc[-1] + 1
                new_row = pd.DataFrame({
                                       'location_id': [pickup_loc_id],
                                       'loc_name': [pickup_loc_name],
                                       })
                locations = pd.concat([locations, new_row], ignore_index=True)

            if dropoff_loc_name in locations.loc_name.values.tolist():
                dropoff_loc_id = (locations[locations.loc_name
                                            == dropoff_loc_name]
                                  ['location_id']
                                  .values[0])
            else:
                dropoff_loc_id = locations['location_id'].iloc[-1] + 1
                new_row = pd.DataFrame({
                                       'location_id': [dropoff_loc_id],
                                       'loc_name': [dropoff_loc_name],
                                       })
                locations = pd.concat([locations, new_row], ignore_index=True)

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
        try:
            if trips.shape[0] == 0:
                row['trip_id'] = 1
            elif row in (trips.loc[:, trips.columns != 'trip_id']
                              .to_dict(orient='records')):
                raise SakayDBError
            else:
                row['trip_id'] = trips['trip_id'].iloc[-1] + 1
            trips = pd.concat([trips, pd.DataFrame.from_dict([row])],
                              ignore_index=True)
        except SakayDBError:
            raise SakayDBError

        trips.to_csv(os.path.join(self.data_dir, 'trips.csv'),
                     index=False)
        drivers.to_csv(os.path.join(self.data_dir, 'drivers.csv'),
                       index=False)
        locations.to_csv(os.path.join(self.data_dir, 'locations.csv'),
                         index=False)

        return trips['trip_id'].iloc[-1]

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