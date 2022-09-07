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
		driver: bar plots

        """
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
            ax.plot(df_final.loc[0], marker='o', label=0)
            ax.plot(df_final.loc[1], marker='o', label=1)
            ax.plot(df_final.loc[2], marker='o', label=2)
            ax.plot(df_final.loc[3], marker='o', label=3)
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

    def generate_odmatrix(self):
        pass
