import pandas as pd
import numpy as np
from helpers import mean_hourly_rate_df
from tqdm import tqdm


class MakeEntity:
    def __init__(self, df, entity):
        """

        :param pandas.DataFrame df:
        :param str entity:
        """
        self.df = df
        self.entity = entity

    def get_entity_features(self):
        # First we will calculate the rates, so let's drop all the NaN
        rate_df = self.df.dropna(subset=['rate'])

        # Calculate the rates by hour and delete the old rate column.
        # Then drop any remaining NaN
        rate_df = rate_df.\
            merge(mean_hourly_rate_df(rate_df),
                  left_on=['_id'], right_on=['_id'])

        # Now get the stats we want for rate
        rate_df = self.calculate_entity_rate_features(rate_df)

        # Get a count of the entities
        df = pd.value_counts(self.df[self.entity]).\
            reset_index().\
            rename(columns={'index': self.entity,
                            self.entity: self.entity+'_count'})

        # Get the unique cities
        # tqdm.pandas(desc='unique_cities')
        # df['unique_cities'] = df['index'].progress_apply(lambda x: self.get_unique_cities(x))
        unique_city_df = self.df.loc[:, [self.entity, 'city']].\
            dropna().\
            drop_duplicates().\
            groupby(self.entity).\
            count().\
            reset_index().\
            rename(columns={'city': 'unique_cities'})
        df = df.merge(unique_city_df, left_on=self.entity, right_on=self.entity)
        del unique_city_df

        # Get the unique states
        # tqdm.pandas(desc='unique_states')
        # df['unique_states'] = df['index'].progress_apply(lambda x: self.get_unique_states(x))
        unique_state_df = self.df.loc[:, [self.entity, 'state']].\
            dropna().\
            drop_duplicates().\
            groupby(self.entity).\
            count().\
            reset_index().\
            rename(columns={'state': 'unique_states'})
        df = df.merge(unique_state_df, left_on=self.entity, right_on=self.entity)
        del unique_state_df

        # Reset the index on our rate dataframe and rename the columns
        rate_df.reset_index(level=0, inplace=True)
        rate_df.columns = [self.entity, 'rate_count', 'rate_mean', 'rate_std', 'rate_median']

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
