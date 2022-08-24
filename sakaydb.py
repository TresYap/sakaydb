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

    def generate_odmatrix(self):
        pass