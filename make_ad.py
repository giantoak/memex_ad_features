import pandas as pd
import pandas
import os
from helpers import mean_hourly_rate_df
from tqdm import tqdm
from config_parser import Parser

quantiles = ['rate_ad_p{}_msa'.format(str(i).zfill(2))
             for i in range(5, 96, 5)]


class MakeAd:
    def __init__(self, city_features, state_features, ad_df):
        self.city_features = city_features
        self.state_features = state_features
        self.ad_df = ad_df
        self.ad_df.drop(self.ad_df.columns[0], axis=1, inplace=True)
        self.ad_df.reset_index(inplace=True)
        #self.ad_df.set_index(keys='_id', inplace=True)
        x = 3
        pass
    def get_ad_features(self):
        """
        Will get the specified ad features
        :returns: `pandas.DataFrame` -- Dataframe containing ad features
        """
        # Since we need the rate to do any calculations
        # drop all values from the ad that do not have a rate
        df = self.ad_df.dropna(subset=['rate'])
        #df = self.ad_df
        imputed_rate_df = self.ad_df.dropna(subset=['imputed_rate'])

        # Calculate the rate per hour
        # then drop the old rate column and get rid of NaN values
        per_hour_df = mean_hourly_rate_df(df)
        df = df.merge(per_hour_df, left_on=['_id'], right_on=['_id'])
        del per_hour_df
        df.drop('rate', axis=1, inplace=True)

        # Since we just got rid of the rate column, let's drop the duplicates
        df.drop_duplicates(inplace=True)


        if len(df) > 0:
            # Now get relative price
            tqdm.pandas(desc='relative_price_to_city')
            df['relative_price_to_city'] = df.progress_apply(
                lambda x: self.calculate_price_relative_loc(x['rate_per_hour'],
                                                            'city',
                                                            x['city_wikidata_id']),
                axis=1)

            tqdm.pandas(desc='relative_price_to_state')
            df['relative_price_to_state'] = df.progress_apply(
                lambda x: self.calculate_price_relative_loc(x['rate_per_hour'],
                                                            'state',
                                                            x['state_wikidata_id']),
                axis=1)

            # Now get relative quantile
            tqdm.pandas(desc='relative_quantile_to_city')
            df['relative_quantile_to_city'] = df.progress_apply(
                lambda x: self.calculate_quantile_relative_loc(x['rate_per_hour'],
                                                               'city',
                                                               x['city_wikidata_id']),
                axis=1)

            tqdm.pandas(desc='relative_quantile_to_state')
            df['relative_quantile_to_state'] = df.progress_apply(
                lambda x: self.calculate_quantile_relative_loc(x['rate_per_hour'],
                                                               'state',
                                                               x['state_wikidata_id']),
                axis=1)

        if len(imputed_rate_df):
            # Now get the imputed relative price
            tqdm.pandas(desc='relative_imputed_price_to_city')
            imputed_rate_df['relative_imputed_price_to_city'] = imputed_rate_df.progress_apply(
                lambda x: self.calculate_price_relative_loc(x['imputed_rate'],
                                                            'city',
                                                            x['city_wikidata_id']),
                axis=1)

            tqdm.pandas(desc='relative_imputed_price_to_state')
            imputed_rate_df['relative_imputed_price_to_state'] = imputed_rate_df.progress_apply(
                lambda x: self.calculate_price_relative_loc(x['imputed_rate'],
                                                            'state',
                                                            x['state_wikidata_id']),
                axis=1)

            # Now get imputed relative quantile
            tqdm.pandas(desc='relative_imputed_quantile_to_city')
            imputed_rate_df['relative_imputed_quantile_to_city'] = imputed_rate_df.progress_apply(
                lambda x: self.calculate_quantile_relative_loc(x['imputed_rate'],
                                                               'city',
                                                               x['city_wikidata_id']),
                axis=1)

            tqdm.pandas(desc='relative_imputed_quantile_to_state')
            imputed_rate_df['relative_imputed_quantile_to_state'] = imputed_rate_df.progress_apply(
                lambda x: self.calculate_quantile_relative_loc(x['imputed_rate'],
                                                               'state',
                                                               x['state_wikidata_id']),
                axis=1)

        total_df = imputed_rate_df.merge(df, how='outer')
        return total_df

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
            df = self.city_features.loc[self.city_features.city_wikidata_id == loc_name]
        elif loc_type == 'state':
            df = self.state_features.loc[self.state_features.state_wikidata_id == loc_name]
        else:
            return None

        # (Price - mean) / standard deviation
        if df.empty:
            return None
        else:
            relative_price = (rate - df.iloc[0]['rate_mean']) / df.iloc[0]['rate_std']
            return relative_price

    def calculate_quantile_relative_loc(self, rate, loc_col, loc_name):
        """
        Gets the quantile the rate is in
        :param float rate: Rate of the ad
        :param str loc_col: Location column to use
        :param str loc_name: name of the location for the specified rate
        :returns: `int` -- Quantile of rate
        """
        if pd.isnull(loc_name):
            return None

        if loc_col == 'city':
            df = self.city_features.loc[self.city_features.city_wikidata_id == loc_name]
        elif loc_col == 'state':
            df = self.state_features.loc[self.state_features.state_wikidata_id == loc_name]
        else:
            return None

        if df.empty:
            return None
        else:
            return (df.iloc[0][quantiles].searchsorted(rate)[0] + 1) * 5


if __name__ == '__main__':
    config = Parser().parse_config('config/config.conf', 'Test')
    # Get the dataframe from the provided file
    file = '/home/gabriel/Documents/Memex/ad_features/location_data/city_id_490584.csv'
    dataframe = pandas.read_csv(file)

    # Get the city and state location info
    city_dataframe = pandas.read_csv('{0}location_characteristics_city.csv'.format(config['result_data']))
    state_dataframe = pandas.read_csv('{0}location_characteristics_state.csv'.format(config['result_data']))

    make_ad = MakeAd(city_dataframe, state_dataframe, dataframe)
    results = make_ad.get_ad_features()
    if os.path.isfile('{0}ad_characteristics.csv'.format(config['result_data'])):
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=False, mode='a', encoding='utf-8')
    else:
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=True, encoding='utf-8')