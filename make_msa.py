import pandas as pd
import numpy as np
import datetime

class MakeMSA:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def get_msa_features(self):
        """
        Will get all features related to the msa
        :return: All msa features
        """

        # Since calculating with NaN is problematic and it slows down the processing, let's create two dataframes. One for rates without NaN and one for ages without NaN
        rate_dataframe = self.dataframe.dropna(subset=['rate'])
        age_dataframe = self.dataframe.dropna(subset=['age'])

        # Calculate the rates by hour and delete the old rate column. Then drop any remaining NaN
        rate_dataframe['rate_per_hour'] = rate_dataframe['rate'].apply(lambda x: self.calculate_rate(x))
        rate_dataframe = rate_dataframe.drop('rate', 1)
        rate_dataframe = rate_dataframe.dropna(subset=['rate_per_hour'])

        # Now do rates and age for city
        city_stats_rate = self.calculate_rate_features(rate_dataframe, 'city')
        city_stats_age = self.calculate_age_features(age_dataframe, 'city')
        city_stats = city_stats_rate.join(city_stats_age, how='outer')

        # Now do rates and age for state
        state_stats_rate = self.calculate_rate_features(rate_dataframe, 'state')
        state_stats_age = self.calculate_age_features(age_dataframe, 'state')
        state_stats = state_stats_rate.join(state_stats_age, how='outer')

        return {'state_stats': state_stats, 'city_stats': city_stats}

    def calculate_rate_features(self, df, column):
        """

        :param df: dataframe
        :param column: Column to perfom the aggragation on
        :return:
        """
        return df.groupby(column)['rate_per_hour'].aggregate({'rate_count': len,
                                                              'rate_mean': np.mean,
                                                              'rate_std': np.std,
                                                              'rate_ad_p05_msa': lambda x: np.percentile(x, q=5),
                                                              'rate_ad_p10_msa': lambda x: np.percentile(x, q=10),
                                                              'rate_ad_p15_msa': lambda x: np.percentile(x, q=15),
                                                              'rate_ad_p20_msa': lambda x: np.percentile(x, q=20),
                                                              'rate_ad_p25_msa': lambda x: np.percentile(x, q=25),
                                                              'rate_ad_p30_msa': lambda x: np.percentile(x, q=30),
                                                              'rate_ad_p35_msa': lambda x: np.percentile(x, q=35),
                                                              'rate_ad_p40_msa': lambda x: np.percentile(x, q=40),
                                                              'rate_ad_p45_msa': lambda x: np.percentile(x, q=45),
                                                              'rate_ad_p50_msa': lambda x: np.percentile(x, q=50),
                                                              'rate_ad_p55_msa': lambda x: np.percentile(x, q=55),
                                                              'rate_ad_p60_msa': lambda x: np.percentile(x, q=60),
                                                              'rate_ad_p65_msa': lambda x: np.percentile(x, q=65),
                                                              'rate_ad_p70_msa': lambda x: np.percentile(x, q=70),
                                                              'rate_ad_p75_msa': lambda x: np.percentile(x, q=75),
                                                              'rate_ad_p80_msa': lambda x: np.percentile(x, q=80),
                                                              'rate_ad_p85_msa': lambda x: np.percentile(x, q=85),
                                                              'rate_ad_p90_msa': lambda x: np.percentile(x, q=90),
                                                              'rate_ad_p95_msa': lambda x: np.percentile(x, q=95)})

    def calculate_age_features(self, df, column):
        """

        :param df:
        :param column: Column to perform the aggragation on
        :return:
        """
        return df.groupby(column)['age'].aggregate({'age_count': len,
                                                    'age_mean': np.mean,
                                                    'age_std': np.std,
                                                    'age_ad_p05_msa': lambda x: np.percentile(x, q=5),
                                                    'age_ad_p10_msa': lambda x: np.percentile(x, q=10),
                                                    'age_ad_p15_msa': lambda x: np.percentile(x, q=15),
                                                    'age_ad_p20_msa': lambda x: np.percentile(x, q=20),
                                                    'age_ad_p25_msa': lambda x: np.percentile(x, q=25),
                                                    'age_ad_p30_msa': lambda x: np.percentile(x, q=30),
                                                    'age_ad_p35_msa': lambda x: np.percentile(x, q=35),
                                                    'age_ad_p40_msa': lambda x: np.percentile(x, q=40),
                                                    'age_ad_p45_msa': lambda x: np.percentile(x, q=45),
                                                    'age_ad_p50_msa': lambda x: np.percentile(x, q=50),
                                                    'age_ad_p55_msa': lambda x: np.percentile(x, q=55),
                                                    'age_ad_p60_msa': lambda x: np.percentile(x, q=60),
                                                    'age_ad_p65_msa': lambda x: np.percentile(x, q=65),
                                                    'age_ad_p70_msa': lambda x: np.percentile(x, q=70),
                                                    'age_ad_p75_msa': lambda x: np.percentile(x, q=75),
                                                    'age_ad_p80_msa': lambda x: np.percentile(x, q=80),
                                                    'age_ad_p85_msa': lambda x: np.percentile(x, q=85),
                                                    'age_ad_p90_msa': lambda x: np.percentile(x, q=90),
                                                    'age_ad_p95_msa': lambda x: np.percentile(x, q=95)})

    def get_rates(self):
        """
        Creates a data frame with a new rates column from the rates column with comma delimted fields
        :return:
        """
        # Get only rates and msa
        df = self.dataframe[['rate', 'city', 'state']].dropna(0)

        # Calculate the rate per hour
        df['rate_per_hour'] = df['rate'].apply(lambda x: self.calculate_rate(x))

        # We don't need the original rate column anymore
        df = df.drop('rate', 1)

        # Drop the nan values and return
        return df.dropna(0)

    def get_ages(self):
        """

        :return: Data frame with MSA and Age
        """
        # Get only ages and msa
        return self.dataframe[['age', 'city']].dropna(0)



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

    def plot_prices(self):
        # First get rates, post date and msa
        df = self.dataframe[['rate', 'post_date', 'msa_name']].dropna(0)

        # Calculate the rate per hour
        df['rate_per_hour'] = df['rate'].apply(lambda x: self.calculate_rate(x))

        # Drop nan once more to get rid of prices we couldn't calculate
        df = df.dropna(0)

        # We don't need the original rate column anymore
        df = df.drop('rate', 1)

        # Convert post date column to datetime
        df['post_date'] = pd.to_datetime(df['post_date'], format='%Y-%m-%d')

        # Change the date to represent quarters
        df['post_date'] = df['post_date'].apply(lambda x: self.get_quarter(x))

        # Group by msa and post date
        df = df.groupby(['msa_name', 'post_date']).filter(lambda x: len(x) > 300)

        # Find the msa with the proper amount of rates


        df['test'] = df['']

    def get_quarter(self, value):
        day = 1
        year = value.year
        month = value.month

        if month > 0 and month < 4:
            month = 1
        elif month > 3 and month < 7:
            month = 4
        elif month > 6 and month < 10:
            month = 7
        else:
            month = 10

        value = pd.Timestamp(datetime.date(year, month, day))
        return value


    """ **** CURRENTLY NOT USED***
    def get_incall_outcall(self):
        # First get only the service and msa from the dataframe
        df = self.dataframe[['service', 'msa_name']]

        # Group by msa
        group_by_msa = df.groupby('msa_name')

        # Get the percentage with incall
        group_by_msa_incall = df[df['service'].str.contains("incall")==True].groupby('msa_name').size() / group_by_msa.size()

        # Get the percentage with outcall
        group_by_msa_outcall = df[df['service'].str.contains("outcall")==True].groupby('msa_name').size() / group_by_msa.size()"""