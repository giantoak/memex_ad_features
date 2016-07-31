import pandas as pd
from helpers import mean_hourly_rate


class MakeAd:
    def __init__(self, city_features, state_features, ad_df):
        self.city_features = city_features
        self.state_features = state_features
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
        # then drop the old rate column and get rid of NaN values
        df['rate_per_hour'] = df['rate'].apply(mean_hourly_rate)
        df = df.dropna(subset=['rate_per_hour'])
        df = df.drop('rate', 1)

        # Now get relative price
        df['relative_price_to_city'] = df.apply(
            lambda x: self.calculate_price_relative_city(x['rate_per_hour'], x['city']), axis=1)
        df['relative_price_to_state'] = df.apply(
            lambda x: self.calculate_price_relative_state(x['rate_per_hour'], x['state']), axis=1)

        # Now get relative quantile
        df['relative_quantile_to_city'] = df.apply(
            lambda x: self.calculate_quantile_relative_city(x['rate_per_hour'], x['city']), axis=1)
        df['relative_quantile_to_state'] = df.apply(
            lambda x: self.calculate_quantile_relative_state(x['rate_per_hour'], x['state']), axis=1)

        return df

    def calculate_price_relative_city(self, rate, city_name):
        """
        Get the price relative to the area
        :param rate: Rate of ad
        :param city_name: city name
        :return:
        """
        if pd.isnull(city_name):
            return None
        else:
            df = self.city_features.loc[self.city_features['city'] == city_name]
            # (Price - mean) / standard deviation
            relative_price = (rate - df.iloc[0]['rate_ad_p50_msa']) / df.iloc[0]['rate_std']
            return relative_price

    def calculate_price_relative_state(self, rate, state_name):
        """
        Get the price relative to the area
        :param rate: Rate of ad
        :param msa_name: state name
        :return:
        """
        if pd.isnull(state_name):
            return None
        else:
            df = self.state_features.loc[self.state_features['state'] == state_name]
            # (Price - mean) / standard deviation
            relative_price = (rate - df.iloc[0]['rate_ad_p50_msa']) / df.iloc[0]['rate_std']
            return relative_price

    def calculate_quantile_relative_city(self, rate, city_name):
        """
        Gets the quantile the rate is in
        :param rate: Rate of the ad
        :param city_name: city for the specified rate
        :return: Quantile of rate
        """
        quantiles = ['rate_ad_p05_msa',
                     'rate_ad_p10_msa',
                     'rate_ad_p15_msa',
                     'rate_ad_p20_msa',
                     'rate_ad_p25_msa',
                     'rate_ad_p30_msa',
                     'rate_ad_p35_msa',
                     'rate_ad_p40_msa',
                     'rate_ad_p45_msa',
                     'rate_ad_p50_msa',
                     'rate_ad_p55_msa',
                     'rate_ad_p60_msa',
                     'rate_ad_p65_msa',
                     'rate_ad_p70_msa',
                     'rate_ad_p75_msa',
                     'rate_ad_p80_msa',
                     'rate_ad_p85_msa',
                     'rate_ad_p90_msa',
                     'rate_ad_p95_msa']

        if pd.isnull(city_name):
            return None
        else:
            df = self.city_features.loc[self.city_features['city'] == city_name]

            # If the rate is less than or equal to the lowest quantile
            if rate <= df.iloc[0]['rate_ad_p05_msa']:
                return 5
            # If the rate is greater than or equal to the highest quantile
            elif rate >= df.iloc[0]['rate_ad_p95_msa']:
                return 95
            # My logic here is to go through each quantile,
            # once we find where the rate is between then return that quantile
            else:
                for i in xrange(1, 17, 1):
                    if rate >= df.iloc[0][quantiles[i]] and rate < df.iloc[0][quantiles[i + 1]]:
                        # Since counting starts at 0, add 1 and then multiply by 5
                        return (i + 1) * 5

    def calculate_quantile_relative_state(self, rate, state_name):
        """
        Gets the quantile the rate is in
        :param rate: Rate of the ad
        :param state_name: state for the specified rate
        :return: Quantile of rate
        """
        quantiles = ['rate_ad_p05_msa',
                     'rate_ad_p10_msa',
                     'rate_ad_p15_msa',
                     'rate_ad_p20_msa',
                     'rate_ad_p25_msa',
                     'rate_ad_p30_msa',
                     'rate_ad_p35_msa',
                     'rate_ad_p40_msa',
                     'rate_ad_p45_msa',
                     'rate_ad_p50_msa',
                     'rate_ad_p55_msa',
                     'rate_ad_p60_msa',
                     'rate_ad_p65_msa',
                     'rate_ad_p70_msa',
                     'rate_ad_p75_msa',
                     'rate_ad_p80_msa',
                     'rate_ad_p85_msa',
                     'rate_ad_p90_msa',
                     'rate_ad_p95_msa']

        if pd.isnull(state_name):
            return None
        else:
            df = self.state_features.loc[self.state_features['state'] == state_name]

            # If the rate is less than or equal to the lowest quantile
            if rate <= df.iloc[0]['rate_ad_p05_msa']:
                return 5
            # If the rate is greater than or equal to the highest quantile
            elif rate >= df.iloc[0]['rate_ad_p95_msa']:
                return 95
            # My logic here is to go through each quantile,
            # once we find where the rate is between then return that quantile
            else:
                for i in xrange(1, 17, 1):
                    if rate >= df.iloc[0][quantiles[i]] and rate < df.iloc[0][quantiles[i + 1]]:
                        # Since counting starts at 0, add 1 and then multiply by 5
                        return (i + 1) * 5
