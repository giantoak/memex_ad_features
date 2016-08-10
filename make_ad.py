import numpy as np
import pandas as pd
from helpers import mean_hourly_rate_df
from tqdm import tqdm

quantiles = ['rate_ad_p{}_msa'.format(str(i).zfill(2))
             for i in range(5, 96, 5)]


def _calculate_quantile_relative_loc(x):
    """
    Gets the quantile the rate is in
    :param x: slice from pandas.DataFrame
    :returns: `int` -- Quantile of rate
    """
    try:
        return np.searchsorted(x[quantiles], x['rate_per_hour'])[0]
    except (IndexError, ValueError):
        return np.nan


class MakeAd:
    def __init__(self, city_features, state_features, ad_df):
        self.features = dict()
        self.features['city'] = city_features
        self.features['state'] = state_features
        self.ad_df = ad_df

    def get_ad_features(self):
        """
        Will get the specified ad features
        :returns: `pandas.DataFrame` -- Dataframe containing ad features
        """
        # Since we need the rate to do any calculations
        # drop all values from the ad that do not have a rate
        df = self.ad_df.dropna(subset=['rate'])

        # Calculate the rate per hour
        # then drop the old rate column and get rid of duplicates
        df = df.\
            merge(mean_hourly_rate_df(df),
                  left_on=['_id'], right_on=['_id']).\
            drop('rate', axis=1).\
            drop_duplicates()

        # Calculate relative price and quantile to locations
        for loc_col in ['city', 'state']:
            loc_wikidata_id = loc_col+'_wikidata_id'
            rel_price_col = 'relative_price_to_'+loc_col
            rel_quantile_col = 'relative_quantile_to_'+loc_col

            loc_df = df.dropna(subset=[loc_col]).\
                loc[:, ['_id', 'rate_per_hour', loc_wikidata_id]].\
                merge(self.features[loc_col].\
                      loc[:, [loc_wikidata_id] +
                             ['rate_mean', 'rate_std'] +
                             quantiles],
                      left_on=[loc_wikidata_id],
                      right_on=[loc_wikidata_id])

            loc_df[rel_price_col] = \
                (loc_df.rate_per_hour - loc_df.rate_mean) / loc_df.rate_std

            tqdm.pandas(desc=rel_quantile_col)
            loc_df[rel_quantile_col] = \
            loc_df.loc[:, ['rate_per_hour'] + quantiles].progress_apply(
                _calculate_quantile_relative_loc, axis=1)

            loc_df = loc_df.loc[:, ['_id',
                                    rel_price_col,
                                    rel_quantile_col]]

            df = df.merge(loc_df,
                          left_on=['_id'], right_on=['_id'], how='left')

        return df

    def calculate_price_relative_loc(self, rate, loc_type, loc_name):
        """
        Returns the rate relative to the location's rates
        :param float rate:
        :param str loc_type:
        :param str loc_name:
        :returns: `float` --
        """
        if pd.isnull(loc_name):
            return None

        if loc_type == 'city':
            df = self.city_features.loc[self.city_features.city == loc_name]
        elif loc_type == 'state':
            df = self.state_features.loc[self.state_features.state == loc_name]
        else:
            return None

        # (Price - mean) / standard deviation
        if df.empty:
            return None
        else:
            relative_price = (rate - df.iloc[0]['rate_ad_p50_msa']) / df.iloc[0]['rate_std']
            return relative_price
