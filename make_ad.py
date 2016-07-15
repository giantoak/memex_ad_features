import pandas

class MakeAd:
    def __init__(self, msa_features, ad_dataframe):
        self.msa_features = msa_features
        self.ad_dataframe = ad_dataframe

    def get_ad_features(self):
        """
        Will get the specified ad features
        :return: Dataframe containing ad features
        """
        # Get only rates from data frame
        df = self.ad_dataframe[['rate']].dropna(0)

        # Calculate the rate per hour
        df['rate_per_hour'] = df['rate'].apply(lambda x: self.calculate_rate(x))
        df = df.dropna(0)

        # Drop rate columns from both dataframes as it's not needed anymore
        df = df.drop('rate', 1)
        self.ad_dataframe = self.ad_dataframe.drop('rate', 1)

        # Join the new dataframe with rates with all of our current information - this way every ad has a rate
        df = df.join(self.ad_dataframe,how='left')

        # Now get relative price
        df['relative_price_to_msa'] = df.apply(lambda x: self.calculate_price_relative(x['rate_per_hour'], x['msa_name']), axis=1)

        # Now get relative quantile
        df['relative_quantile_to_msa'] = df.apply(lambda x: self.calculate_quantile_relative(x['rate_per_hour'], x['msa_name']), axis=1)

        return df


    def calculate_price_relative(self, rate, msa_name):
        """
        Get the price relative to the area
        :param rate: Rate of ad
        :param msa_name: MSA name
        :return:
        """
        if pandas.isnull(msa_name):
            return None
        else:
            df = self.msa_features.loc[self.msa_features['msa_name'] == msa_name]
            # (Price - mean) / standard deviation
            relative_price = (rate - df.iloc[0]['rate_ad_p50_msa']) / df.iloc[0]['rate_std']
            return relative_price

    def calculate_quantile_relative(self, rate, msa_name):
        """
        Gets the quantile the rate is in
        :param rate: Rate of the ad
        :param msa_name: MSA for the specified rate
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

        if pandas.isnull(msa_name):
            return None
        else:
            df = self.msa_features.loc[self.msa_features['msa_name'] == msa_name]

            # If the rate is less than or equal to the lowest quantile
            if rate <= df.iloc[0]['rate_ad_p05_msa']:
                return 5
            # If the rate is greater than or equal to the highest quantile
            elif rate >= df.iloc[0]['rate_ad_p95_msa']:
                return 95
            # My logic here is to go through each quantile, once we find where the rate is between then return that quantile
            else:
                for i in xrange(1, 17, 1):
                    if rate >= df.iloc[0][quantiles[i]] and rate < df.iloc[0][quantiles[i + 1]]:
                        # Since counting starts at 0, add 1 and then multiply by 5
                        return (i + 1) * 5


    def calculate_rate(self, rate):
        """
        Takes the comma delimited rate and calculates the new rate
        :param rate: Comma delimted rate from rate file
        :return: Hourly rate
        """
        # Split the rate by comma leaving the price and the unit.
        rate_info = rate.split(',')
        price = rate_info[0]
        # Remove any currency characters from the price
        price = self.strip_characters(price)

        # Make sure the price is a number
        if self.is_valid_number(price):
            unit_info = rate_info[1].split(' ')
            unit = unit_info[0]
            type = unit_info[1]

            if type == 'MINS':
                return (60 / float(unit)) * float(price)
            elif type == 'HOUR':
                return float(price)
            elif type == 'HOURS':
                return float(price) / float(unit)
        else:
            return None

    def strip_characters(self, value):
        """
        Strips common characters that mean 'dollar' from the rate
        :param value:
        :return: Rate without common characters
        """
        replace_characters = ['$', 'roses', 'rose', 'bucks', 'kisses', 'kiss', 'dollars', 'dollar', 'dlr']
        for char in replace_characters:
            if char in value:
                value = value.replace(char, '')

        return value

    def is_valid_number(self, value):
        """
        Checks if a number is valid
        :param value: Test number
        :return: True if valid, false otherwise
        """
        try:
            float(value)
        except ValueError:
            return False

        return True