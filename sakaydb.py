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

    def delete_trip(self, trip_id):

        df = pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))

        cond = trip_id in df['trip_id']
        if not cond:
            raise SakayDBError

        else:
            df.drop(df.index[df['trip_id'] == trip_id], inplace=True)

        return df.to_csv('trips.csv', index=False)

    def search_trips(self, **kwargs):

        df = pd.read_csv(os.path.join(self.data_dir, 'trips.csv'))
        df1 = df
        dfp = df1['pickup_datetime']
        dfd = df1['dropoff_datetime']

        for key, val in kwargs.items():
            if (key != 'driver_id' and
                key != 'pickup_datetime' and
                key != 'dropoff_datetime' and
                key != 'passenger_count' and
                key != 'trip_distance' and
                    key != 'fare_amount'):
                raise SakayDBError

            elif (type(val) != int and
                    type(val) != tuple and
                    type(val) != float and
                    type(val) != str):
                raise SakayDBError

            elif (type(val) == tuple and
                    len(val) != 2):
                raise SakayDBError

            else:

                if (key == 'driver_id' or
                    key == 'passenger_count' or
                    key == 'trip_distance' or
                        key == 'fare_amount'):

                    if (type(val) == int or
                            type(val) == float):

                        df_vals = df1[df1[key] == val]
                        df_merge = pd.merge(df1, df_vals)

                        df1 = df_merge

                    elif type(val) == tuple:

                        key_order = key

                        if (val[0] is None and
                                val[1] is None):
                                    raise SakayDBError

                        elif val[0] is None:
                            val1, val2 = df1[key].min(), val[1]
                        elif val[1] is None:
                            val1, val2 = val[0], df1[key].max()
                        else:
                            val1, val2 = val[0], val[1]

                        df_vals = (df1.loc[df1[key].between(val1, val2)])
                        df_merge = (pd.merge(df1, df_vals)
                                    .sort_values(key_order))

                        df1 = df_merge

                elif (key == 'pickup_datetime' or
                        key == 'dropoff_datetime'):

                    df1['pickup_datetime'] = (
                        pd.to_datetime(df1['pickup_datetime'],
                                       format="%H:%M:%S,%d-%m-%Y")
                                )
                    df1['dropoff_datetime'] = (
                        pd.to_datetime(df1['dropoff_datetime'],
                                       format="%H:%M:%S,%d-%m-%Y"))

                    if type(val) == str:

                        if type(pd.to_datetime(val, errors='coerce',
                                format="%H:%M:%S,%d-%m-%Y")) == (
                                pd._libs.tslibs.nattype.NaTType):
                                    raise SakayDBError

                        val = pd.to_datetime(val,
                                             format="%H:%M:%S,%d-%m-%Y",
                                             errors='coerce')

                        df_vals = df1[df1[key] == val]
                        df_merge = pd.merge(df1, df_vals)

                        df1 = df_merge

                    if type(val) == tuple:

                        key_order = key

                        if type(pd.to_datetime(val[0], errors='coerce',
                                format="%H:%M:%S,%d-%m-%Y")) == (
                                pd._libs.tslibs.nattype.NaTType):
                                    raise SakayDBError

                        elif type(pd.to_datetime(val[1], errors='coerce',
                                  format="%H:%M:%S,%d-%m-%Y")) == (
                                  pd._libs.tslibs.nattype.NaTType):
                                    raise SakayDBError

                        elif (val[0] is None and
                                val[1] is None):
                                    raise SakayDBError

                        elif val[0] is None:
                            val1, val2 = (
                                df1[key].min(),
                                pd.to_datetime(val[1],
                                               format="%H:%M:%S,%d-%m-%Y"))
                        elif val[1] is None:
                            val1, val2 = (
                                pd.to_datetime(val[0],
                                               format="%H:%M:%S,%d-%m-%Y"),
                                df1[key].max())
                        else:
                            val1, val2 = (
                                pd.to_datetime(val[0],
                                               format="%H:%M:%S,%d-%m-%Y"),
                                pd.to_datetime(val[1],
                                               format="%H:%M:%S,%d-%m-%Y"))

                        df_vals = df1.loc[df1[key].between(val1, val2)]
                        df_merge = (pd.merge(df1, df_vals)
                                    .sort_values(key_order))

                        df1 = df_merge

                        for i, count in enumerate(df_vals.index):
                            df1.loc[i, 'pickup_datetime'] = dfp[count]
                            df1.loc[i, 'dropoff_datetime'] = dfd[count]

        return df1

    def export_data(self):
        pass

    def generate_statistics(self):
        pass

    def plot_statistics(self):
        pass

    def generate_odmatrix(self):
        pass
