import pandas as pd
import numpy as np
from helpers import mean_hourly_rate


class MakeEntity:
    def __init__(self, df, entity):
        """

        :param pandas.DataFrame df:
        :param str entity:
        """
        self.df = df
        self.entity = entity

    def get_entity_features(self):
        # First we will calculate the rates, so lets drop all the NaN
        rate_df = self.df.dropna(subset=['rate'])

        # Calculate the rates by hour and delete the old rate column.
        # Then drop any remaining NaN

        rate_df['rate_per_hour'] = rate_df['rate'].apply(mean_hourly_rate)
        rate_df = rate_df.drop('rate', 1)
        rate_df = rate_df.dropna(subset=['rate_per_hour'])

        # Now get the stats we want for rate
        rate_df = self.calculate_entity_rate_features(rate_df)

        # Get a count of the ads
        df = pd.value_counts(self.df[self.entity]).to_frame()
        # Reset the index so we can use the entity column
        df.reset_index(level=0, inplace=True)

        # Get the unique locations
        df['unique_cities'] = df['index'].apply(lambda x: self.get_unique_cities(x))
        df['unique_states'] = df['index'].apply(lambda x: self.get_unique_states(x))

        # Now give the columns the proper names as they have changed
        df.columns = ['phone', 'phone_count', 'unique_cities', 'unique_states']

        # Reset the index on our rate dataframe and rename the columns
        rate_df.reset_index(level=0, inplace=True)
        rate_df.columns = ['phone', 'rate_count', 'rate_mean', 'rate_std', 'rate_median']

        # Lastly merge the two dataframes
        return df.merge(rate_df, how='outer')

        # Save this code as we may use it later
        """df['incall_count'] = df['index'].apply(lambda x: self.get_incall_count(x))
        df['outcall_count'] = df['index'].apply(lambda x: self.get_outcall_count(x))"""

    def calculate_entity_rate_features(self, df_entity_rates):
        """

        :param pandas.DataFrame df_entity_rates:
        :return: MSA rate features
        """
        return df_entity_rates.groupby(self.entity)['rate_per_hour'].agg(
            {'rate_count': len,
             'rate_mean': np.mean,
             'rate_std': np.std,
             'rate_median': lambda x: np.percentile(x, q=50)})

    def get_unique_cities(self, value):
        return self.df.loc[(self.df[self.entity] == value), 'city'].nunique()

    def get_unique_states(self, value):
        return self.df.loc[(self.df[self.entity] == value), 'state'].nunique()

    # Save this code as we may use it later
    """def get_incall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('incall')).sum()
        #TODO only evaluate true

    def get_outcall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('outcall')).sum()"""
