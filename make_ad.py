import pandas

class MakeAd:
    def __init__(self, city_features, state_features, ad_dataframe):
        self.city_features = city_features
        self.state_features = state_features
        self.ad_dataframe = ad_dataframe

    def get_ad_features(self):
        """
        Will get the specified ad features
        :return: Dataframe containing ad features
        """
        # Since we need the rate to do any calculations drop all values from the ad that do not have a rate
        dataframe = self.ad_dataframe.dropna(subset=['rate'])

        # Calculate the rate per hour then drop the old rate column and get rid of NaN values
        dataframe['rate_per_hour'] = dataframe['rate'].apply(lambda x: self.calculate_rate(x))
        dataframe = dataframe.dropna(subset=['rate_per_hour'])
        dataframe = dataframe.drop('rate', 1)

        # Now get relative price
        dataframe['relative_price_to_city'] = dataframe.apply(lambda x: self.calculate_price_relative_city(x['rate_per_hour'], x['city']), axis=1)
        dataframe['relative_price_to_state'] = dataframe.apply(lambda x: self.calculate_price_relative_state(x['rate_per_hour'], x['state']), axis=1)

        # Now get relative quantile
        dataframe['relative_quantile_to_city'] = dataframe.apply(lambda x: self.calculate_quantile_relative_city(x['rate_per_hour'], x['city']), axis=1)
        dataframe['relative_quantile_to_state'] = dataframe.apply(lambda x: self.calculate_quantile_relative_state(x['rate_per_hour'], x['state']), axis=1)

        return dataframe


    def calculate_price_relative_city(self, rate, city_name):
        """
        Get the price relative to the area
        :param rate: Rate of ad
        :param city_name: city name
        :return:
        """
        if pandas.isnull(city_name):
            return None
        else:
            df = self.city_features.loc[self.city_features['city'] == city_name]
            if len(df) == 0:
                return None
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
        if pandas.isnull(state_name):
            return None
        else:
            df = self.state_features.loc[self.state_features['state'] == state_name]
            if len(df) == 0:
                return None
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

        if pandas.isnull(city_name):
            return None
        else:
            dataframe = self.city_features.loc[self.city_features['city'] == city_name]
            if len(dataframe) == 0:
                return None

            # If the rate is less than or equal to the lowest quantile
            if rate <= dataframe.iloc[0]['rate_ad_p05_msa']:
                return 5
            # If the rate is greater than or equal to the highest quantile
            elif rate >= dataframe.iloc[0]['rate_ad_p95_msa']:
                return 95
            # My logic here is to go through each quantile, once we find where the rate is between then return that quantile
            else:
                for i in xrange(1, 17, 1):
                    if rate >= dataframe.iloc[0][quantiles[i]] and rate < dataframe.iloc[0][quantiles[i + 1]]:
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

        if pandas.isnull(state_name):
            return None
        else:
            dataframe = self.state_features.loc[self.state_features['state'] == state_name]
            if len(dataframe) == 0:
                return None

            # If the rate is less than or equal to the lowest quantile
            if rate <= dataframe.iloc[0]['rate_ad_p05_msa']:
                return 5
            # If the rate is greater than or equal to the highest quantile
            elif rate >= dataframe.iloc[0]['rate_ad_p95_msa']:
                return 95
            # My logic here is to go through each quantile, once we find where the rate is between then return that quantile
            else:
                for i in xrange(1, 17, 1):
                    if rate >= dataframe.iloc[0][quantiles[i]] and rate < dataframe.iloc[0][quantiles[i + 1]]:
                        # Since counting starts at 0, add 1 and then multiply by 5
                        return (i + 1) * 5

    def calculate_rate(self, rate):
        """
        Takes the comma delimited rate and calculates the new rate
        :param rate: Comma delimted rate from rate file
        :return: Hourly rate
        """

        if type(rate) is list:
            calculated_rates = []
            for r in rate:
                # Split the rate by comma leaving the price and the unit.
                rate_info = r.split(',')
                price = rate_info[0]
                # Remove any currency characters from the price
                price = self.strip_characters(price)

                # Make sure the price is a number
                if self.is_valid_number(price):
                    unit_info = rate_info[1].split(' ')
                    unit = unit_info[0]
                    duration = unit_info[1]

                    if duration == 'MINS':
                        calculated_rates.append((60 / float(unit)) * float(price))
                    elif duration == 'HOUR':
                        # If it's an hour stop calculating and return it
                        return float(price)
                    elif duration == 'HOURS':
                        calculated_rates.append(float(price) / float(unit))

            # Now average our rates
            if calculated_rates:
                return sum(calculated_rates) / float(len(calculated_rates))
            else:
                return None
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
