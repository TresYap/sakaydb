import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime


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
        """
        Function will add a new trip to trips.csv if input is valid,
        while adding new locations and drivers to the corresponding
        csvs if they are new.

        Parameters
        ----------
        driver
            Full name of the driver of the trip with the form
            Last name, Given name.
        pickup_datetime
            Time and date of trip pickup.
        dropoff_datetime
            Time and date of trip dropoff.
        passenger_count
            The number of passengers in the trip.
        pickup_loc_name
            The name of the pickup location.
        dropoff_loc_name
            The name of the destination location.
        trip_distance
            Total distance of trip in meters.
        fare_amount
            Amount paid for the trip.

        Returns
        -------
        int
            The ID number of the added trip in trips.csv.
        """
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

    def add_trips(self, trips):
        """
        Function will add multiple trips to to trips.csv. This is
        an extension to add_trip for multiple trips. Errors will
        be raised if the trip is either invalid or already exists.

        Parameters
        ----------
        trips : list
            List of dictionaries containing inputs for the add_trip
            function. The contents of each dictionary should be
            valid or errors will be raised.

        Returns
        -------
        list
            List of trip_ids that were successfully addd to the
            trips.csv file.
        """
        out = []
        for i, trip in enumerate(trips):
            try:
                out.append(self.add_trip(**trip))
            except SakayDBError:
                print(f"Warning: trip index {i} is already in the "
                      "database. Skipping...")
            except Exception as e:
                print(f"Warning: trip index {i} has invalid or "
                      "incomplete information. Skipping...")
        return out

    def delete_trip(self):
        pass

    def search_trips(self):
        pass

    def export_data(self):
        pass

    def generate_statistics(self):
        pass
        
    def plot_statistics(self, stat):
        """
        This method takes in a string input as the stat parameter.

        Note
        ----------
        The stat values are case-sensitive and do not add 'self' parameter
        in the Parameters section.

        Parameters
        ----------
        stat
            trip:
                When the parameter is set to 'trip', the method will show
                the average number of trips per day of week.
            passenger:
                When the parameter is set to 'passenger', the method will show
                the average passenger count per day.
            driver:
                When the parameter is set to 'driver', the method will show
                the drivers with the top average trips per day.

        Returns
        ----------
        matplotlib Axes
            depending on the stat parameter passed to it:
            trip: bar plot
            passenger: line plots
            driver: bar plots"""

        if stat == 'trip':
            df_trips = (
                pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
            )

            df_trips['pickup_datetime'] = (
                pd.to_datetime(df_trips['pickup_datetime'],
                               format='%X,%d-%m-%Y')
            )

            df_trips_new = (
                df_trips.groupby('pickup_datetime')
                ['trip_id'].nunique().reset_index()
            )

            df_trips_new.set_index('pickup_datetime', inplace=True)
            df_trips_fin = df_trips_new.resample('D').sum().reset_index()

            df_trips_fin['day_of_week'] = (
                df_trips_fin['pickup_datetime'].dt.day_name()
            )

            df_trips_mean = (
                df_trips_fin.groupby('day_of_week')
                ['trip_id'].mean().reset_index()
            )

            days_in_nums = {'Monday': 0, 'Tuesday': 1,
                            'Wednesday': 2, 'Thursday': 3,
                            'Friday': 4, 'Saturday': 5, 'Sunday': 6}

            df_trips_mean['days_in_nums'] = (
                df_trips_mean['day_of_week'].map(days_in_nums)
            )

            df_trips_sorted = (
                df_trips_mean.sort_values(by=['days_in_nums'],
                                          ascending=True)
            )

            df_trips_sorted.drop("days_in_nums",
                                 axis=1, inplace=True)
            df_trips_sorted.set_index('day_of_week', inplace=True)

            graph = (
                df_trips_sorted.plot(legend=None,
                                     kind='bar', figsize=(12, 8))
            )

            y_range = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45]
            y_range1 = ['0', '5', '10', '15', '20',
                        '25', '30', '35', '40', '45']

            graph.yaxis.set_ticks(y_range)
            graph.yaxis.set_ticklabels(y_range1)
            graph.set_title('Average trips per day')
            graph.set_ylabel('Ave Trips')
            graph.set_xlabel('Day of week')
            plt.show()
            return graph

        elif stat == 'passenger':
            df_trips = (
                pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
            )

            df_trips['pickup_datetime'] = (
                pd.to_datetime(df_trips['pickup_datetime'],
                               format='%X,%d-%m-%Y')
            )

            df_trial = (
                df_trips.groupby(['pickup_datetime', 'passenger_count'])
                ['trip_id'].nunique()
            )

            df_trial1 = (
                df_trial.groupby(
                    [pd.Grouper(level='passenger_count'),
                     pd.Grouper(level='pickup_datetime',
                                freq='D')]).sum().reset_index()
            )

            df_trial1['day_of_week'] = (
                df_trial1['pickup_datetime'].dt.day_name()
            )

            df_fin = (
                df_trial1.groupby(['passenger_count', 'day_of_week'])
                ['trip_id'].mean().reset_index()
            )

            days_in_nums = {'Monday': 0, 'Tuesday': 1,
                            'Wednesday': 2, 'Thursday': 3,
                            'Friday': 4, 'Saturday': 5, 'Sunday': 6}

            df_fin['days_in_nums'] = (
                df_fin['day_of_week'].map(days_in_nums)
            )

            df_fin.set_index(['passenger_count',
                              'day_of_week', 'days_in_nums'],
                             inplace=True)

            df_final = (
                df_fin.sort_index(level='days_in_nums').reset_index()
            )

            df_final.drop("days_in_nums",
                          axis=1, inplace=True)

            df_final.set_index(['passenger_count',
                                'day_of_week'], inplace=True)

            fig, ax = plt.subplots(figsize=(12, 8))

            number = df_trial1['passenger_count'].unique()
            number = np.sort(number)

            for i in range(len(number)):
                ax.plot(df_final.loc[i], marker='o', label=i)
                ax.set_yticks([9.0, 9.25, 9.5, 9.75, 10.0, 10.25,
                               10.5, 10.75, 11.0, 11.25, 11.5])
                ax.set_xlabel('Day of week')
                ax.set_ylabel('Ave Trips')
                ax.legend()
                fig.canvas.draw()

            return ax

        elif stat == 'driver':
            df_trips = (
                pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
            )

            df_drivers = (
                pd.read_csv(os.path.join(self.data_dir, 'drivers.csv'))
            )

            df_trips['pickup_datetime'] = (
                pd.to_datetime(df_trips['pickup_datetime'],
                               format='%X,%d-%m-%Y')
            )

            df_drivers["full_name"] = (
                df_drivers["given_name"] + ' ' + df_drivers["last_name"]
            )

            names = dict(zip(df_drivers['driver_id'], df_drivers['full_name']))

            df_driv = (
                df_trips.groupby(['pickup_datetime', 'driver_id'])
                ['trip_id'].nunique()
            )

            df_driv1 = (
                df_driv.groupby([pd.Grouper(level='driver_id'),
                                 pd.Grouper(level='pickup_datetime',
                                            freq='D')]).sum().reset_index()
            )

            df_driv1['day_of_week'] = (
                df_driv1['pickup_datetime'].dt.day_name()
            )

            df_driv2 = (
                df_driv1.groupby(['driver_id', 'day_of_week'])
                ['trip_id'].mean().reset_index()
            )

            days_in_nums = {'Monday': 0, 'Tuesday': 1,
                            'Wednesday': 2, 'Thursday': 3,
                            'Friday': 4, 'Saturday': 5, 'Sunday': 6}

            df_driv2['days_in_nums'] = (
                df_driv2['day_of_week'].map(days_in_nums)
            )

            df_driv2['driver'] = df_driv2['driver_id'].map(names)
            df_driv3 = (
                df_driv2.sort_values([
                    'days_in_nums', 'trip_id', 'driver'],
                    ascending=[True, False, True]).reset_index(drop=True)
            )

            df_driv3.set_index(['days_in_nums'], inplace=True)

            row_count = 7
            column_count = 1
            h_space = 0.2
            fig, ax = plt.subplots(row_count, column_count,
                                   figsize=(8, 25), sharex=True)
            fig.subplots_adjust(hspace=h_space)

            for i in range(row_count):
                ax[i].barh(df_driv3.loc[i].head(5)['driver'],
                           df_driv3.loc[i].head(5)['trip_id'],
                           label=df_driv3['day_of_week'].unique()[i])
                ax[i].legend()
                ax[i].invert_yaxis()

            fig.canvas.draw()
            return fig
        else:
            raise SakayDBError

    def generate_odmatrix(self, date_range=(None, None)):
        """Create a method generate_odmatrix that takes in a date_range input
        parameter and returns a pandas.DataFrame with the trips.csv
        pickup_loc_name as the row names (dataframe index) and dropoff_loc_name
        as the columns. The values for each row-column combination is the
        average daily number of trips that occured within the date_range
        specified.

        Parameters
        ----------
        date_range
            Takes a tuple of datetime strings, and filters trips based
            on pickup_datetime. Defaults to None, in which case all dates
            are included.

            Case 1: tuple like (value, None) sorts by key (chronological
            or ascending) returns all entries from value, begin inclusive
            Case 2: tuple like (None,value) sorts by key (chronological or
            ascending) returns all entries up to value, end inclusive
            Case 3: tuple like (value1, value2) sorts by key and returns values
            between value1 and value2, end inclusive.

            Input errors to the date_range parameter should be handled like
            that of search_trips.

        Returns
        -------
        dataframe
            Return a pandas.DataFrame with the trips.csv pickup_loc_name as
            the row names (dataframe index) and dropoff_loc_name as the
            columns. The values for each row-column combination is the average
            daily number of trips that occured within the date_range
            specified.

        """

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
