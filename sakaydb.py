import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

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

    def generate_statistics(self, stat):
        """
        Function will generate different statistics based on the stat
        parameter passed.
        
        Parameters
        ----------
        stat : str
            trip : Returns the average number of trips with pickups per day.
            
            passenger : Returns the average number of trips per day per passenger count.
            
            driver : Returns the average number of trips per day per driver.
            
            all : Keys for dict are trip, passenger, and driver with the corresponding stat.

        Returns
        -------
        dict
            Dictionary containing the required stats.
        """
        if (not os.path.isfile(os.path.join(self.data_dir, 'trips.csv'))
            or not os.path.isfile(os.path.join(self.data_dir, 'drivers.csv'))
            or not os.path.isfile(os.path.join(self.data_dir, 'locations.csv'))):

            if stat in ['trip', 'passenger', 'driver']:
                return {}
            elif stat == 'all':
                return {'trip': {}, 'passenger': {}, 'driver': {}}
            else:
                raise SakayDBError
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
        df = df[['trip_id',
                 'driver_lastname',
                 'driver_givenname',
                 'pickup_datetime',
                 'dropoff_datetime',
                 'passenger_count',
                 'pickup_loc_name',
                 'dropoff_loc_name',
                 'trip_distance',
                 'fare_amount']]

        df.pickup_datetime = pd.to_datetime(df.pickup_datetime, format='%H:%M:%S,%d-%m-%Y')
        df.dropoff_datetime = pd.to_datetime(df.dropoff_datetime, format='%H:%M:%S,%d-%m-%Y')

        df['day'] = df['pickup_datetime'].dt.day_name()
        dfc = df.copy()

        # para case insensitive:
        dfc['day'] = dfc['day'].str.title()
        dfc['driver'] = dfc['driver_lastname'].str.title()+', '+dfc['driver_givenname'].str.title()

        week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if (stat != 'trip') & (stat != 'passenger') & (stat != 'driver') & (stat != 'all'):
            raise SakayDBError

        elif stat == 'trip':
            trip_ = {}
            for w in week:
                dfc_filter = (dfc.groupby(['day',
                                           pd.Grouper(key='pickup_datetime', freq='D')])
                              ['trip_id']
                              .count().reset_index())
                dfc_filter_trip = dfc_filter.groupby('day')['trip_id'].mean()
                trip = {w:dfc_filter_trip[w]}
                trip_.update(**trip)
            return trip_

        elif stat == 'passenger':
            number = dfc['passenger_count'].unique()
            number = np.sort(number)
            passenger_ = {}
            daily_ = {}
            for n in number:
                # Filter df by unique passenger count
                dfc_filter = dfc.loc[dfc['passenger_count'] == n]
                # Daily bins
                dfc_filter = dfc_filter.groupby(['day', pd.Grouper(key='pickup_datetime', freq='D')]).count().reset_index()
                # Get mean by day name
                dfc_filter_pass = dfc_filter.groupby('day')['passenger_count'].mean().reindex(week)
                # Convert to dictionary
                daily_ = dfc_filter_pass.to_dict()
                daily_.update(daily_, **daily_)
                passenger_[n] = daily_
            return passenger_

        elif stat == 'driver':
            name = dfc['driver'].unique()
            driver_ = {}
            daily_ = {}
            for n in name:
                # Filter by driver name
                dfc_filter = dfc.loc[dfc['driver'] == n]
                # Daily bins
                dfc_filter = dfc_filter.groupby(['day', pd.Grouper(key='pickup_datetime', freq='D')]).count().reset_index()
                # Get mean by day name
                dfc_filter_driver = dfc_filter.groupby('day')['driver'].mean().reindex(week)
                daily_ = dfc_filter_driver.to_dict()
                daily_.update(daily_, **daily_)
                driver_[n] = daily_
            return driver_

        elif stat == 'all':
            trip_ = {}
            for w in week:
                dfc_filter = dfc.groupby(['day', pd.Grouper(key='pickup_datetime', freq='D')])['trip_id'].count().reset_index()
                dfc_filter_trip = dfc_filter.groupby('day')['trip_id'].mean()
                trip = {w:dfc_filter_trip[w]}
                trip_.update(**trip)

            number = dfc['passenger_count'].unique()
            number = np.sort(number)
            passenger_ = {}
            daily_ = {}
            for n in number:
                # Filter df by unique passenger count
                dfc_filter = dfc.loc[dfc['passenger_count'] == n]
                # Daily bins
                dfc_filter = dfc_filter.groupby(['day', pd.Grouper(key='pickup_datetime', freq='D')]).count().reset_index()
                # Get mean by day name
                dfc_filter_pass = dfc_filter.groupby('day')['passenger_count'].mean().reindex(week)
                # Convert to dictionary
                daily_ = dfc_filter_pass.to_dict()
                daily_.update(daily_, **daily_)
                passenger_[n] = daily_

            name = dfc['driver'].unique()
            driver_ = {}
            daily_ = {}
            for n in name:
                # Filter by driver name
                dfc_filter = dfc.loc[dfc['driver'] == n]
                # Daily bins
                dfc_filter = dfc_filter.groupby(['day', pd.Grouper(key='pickup_datetime', freq='D')]).count().reset_index()
                # Get mean by day name
                dfc_filter_driver = dfc_filter.groupby('day')['driver'].mean().reindex(week)
                daily_ = dfc_filter_driver.to_dict()
                daily_.update(daily_, **daily_)
                driver_[n] = daily_

            dict_all = {'trip':trip_, 'passenger':passenger_, 'driver':driver_}
            return dict_all

    def plot_statistics(self):
        pass

    def generate_odmatrix(self):
        pass
