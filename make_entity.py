import pandas as pd
import numpy as np
from helpers import mean_hourly_rate_df


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
        rate_df = rate_df.\
            merge(mean_hourly_rate_df(rate_df),
                  left_on=['_id'], right_on=['_id']).\
            drop('rate', axis=1).\
            drop_duplicates()

        # Now get the stats we want for rate
        rate_df = self.calculate_entity_rate_features(rate_df)

        # Get a count of the entities
        df = pd.value_counts(self.df[self.entity]).\
            reset_index().\
            rename(columns={'index': self.entity,
                            self.entity: self.entity+'_count'})

        # Get counts of unique locations
        for loc_col, unique_loc_col in [('city_wikidata_id',
                                         'unique_cities'),
                                        ('state_wikidata_id',
                                         'unique_states')]:
            unique_loc_df = self.df.loc[:, [self.entity, loc_col]].\
                dropna().\
                drop_duplicates().\
                groupby(self.entity).\
                count().\
                reset_index().\
                rename(columns={loc_col: unique_loc_col})

            df = df.merge(unique_loc_df,
                          how='left',
                          left_on=self.entity,
                          right_on=self.entity)

            df.loc[:, unique_loc_col] = \
                df.loc[:, unique_loc_col].fillna(0).astype(int)

            del unique_loc_df

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

    # def get_unique_cities(self, value):
    #     return self.df.loc[(self.df[self.entity] == value), 'city'].nunique()

    # def get_unique_states(self, value):
    #     return self.df.loc[(self.df[self.entity] == value), 'state'].nunique()

    # Save this code as we may use it later
    """def get_incall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('incall')).sum()
        #TODO only evaluate true

    def get_outcall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('outcall')).sum()"""
