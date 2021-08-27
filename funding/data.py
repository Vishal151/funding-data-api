import os
import pandas as pd

class VCFunded:

    def get_data(self):
        """
        This function returns a Python dict.
        Its values are pandas.DataFrame loaded from csv files
        """

        root_dir = os.path.dirname(os.path.dirname(__file__))
        csv_path = os.path.join(root_dir,'data')
        file_names = [f for f in os.listdir(csv_path) if f.endswith('.csv')]

        key_names = [
            key_name.replace('funding_','').replace('.csv','') 
            for key_name in file_names]

        data = {}
        for k,f in zip(key_names, file_names):
            data[k] = pd.read_csv(os.path.join(csv_path, f))
        return data

    def ping(self):
        """
        You call ping I print pong.
        """
        print('pong')