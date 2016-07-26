import pandas
import numpy
import datetime

class MakeMSA:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def get_msa_features(self):
        """
        Will get all features related to the msa
        :return: All msa features
        """

        # Get the rates
        msa_rate = self.get_rates()
        # Get the rate features
        df_msa_rate = self.calculate_msa_rate_features(msa_rate)
        # Get the ages
        msa_age = self.get_ages()
        # Get the age features
        df_msa_age = self.calculate_msa_age_features(msa_age)
        # Join the two and return
        return df_msa_rate.join(df_msa_age, how='outer')

    def calculate_msa_rate_features(self, df_msa_rates):
        """

        :param df_msa_rates:
        :return: MSA rate features
        """
        return df_msa_rates.groupby('msa_name')['rate_per_hour'].aggregate({'rate_count': len,
                                                                            'rate_mean': numpy.mean,
                                                                            'rate_std': numpy.std,
                                                                            'rate_ad_p05_msa': lambda x: numpy.percentile(x, q=5),
                                                                            'rate_ad_p10_msa': lambda x: numpy.percentile(x, q=10),
                                                                            'rate_ad_p15_msa': lambda x: numpy.percentile(x, q=15),
                                                                            'rate_ad_p20_msa': lambda x: numpy.percentile(x, q=20),
                                                                            'rate_ad_p25_msa': lambda x: numpy.percentile(x, q=25),
                                                                            'rate_ad_p30_msa': lambda x: numpy.percentile(x, q=30),
                                                                            'rate_ad_p35_msa': lambda x: numpy.percentile(x, q=35),
                                                                            'rate_ad_p40_msa': lambda x: numpy.percentile(x, q=40),
                                                                            'rate_ad_p45_msa': lambda x: numpy.percentile(x, q=45),
                                                                            'rate_ad_p50_msa': lambda x: numpy.percentile(x, q=50),
                                                                            'rate_ad_p55_msa': lambda x: numpy.percentile(x, q=55),
                                                                            'rate_ad_p60_msa': lambda x: numpy.percentile(x, q=60),
                                                                            'rate_ad_p65_msa': lambda x: numpy.percentile(x, q=65),
                                                                            'rate_ad_p70_msa': lambda x: numpy.percentile(x, q=70),
                                                                            'rate_ad_p75_msa': lambda x: numpy.percentile(x, q=75),
                                                                            'rate_ad_p80_msa': lambda x: numpy.percentile(x, q=80),
                                                                            'rate_ad_p85_msa': lambda x: numpy.percentile(x, q=85),
                                                                            'rate_ad_p90_msa': lambda x: numpy.percentile(x, q=90),
                                                                            'rate_ad_p95_msa': lambda x: numpy.percentile(x, q=95)})

    def calculate_msa_age_features(self, df_msa_ages):
        """

        :param df_msa_ages:
        :return: Age features
        """
        return df_msa_ages.groupby('msa_name')['age'].aggregate({'age_count': len,
                                                                 'age_mean': numpy.mean,
                                                                 'age_std': numpy.std,
                                                                 'age_ad_p05_msa': lambda x: numpy.percentile(x, q=5),
                                                                 'age_ad_p10_msa': lambda x: numpy.percentile(x, q=10),
                                                                 'age_ad_p15_msa': lambda x: numpy.percentile(x, q=15),
                                                                 'age_ad_p20_msa': lambda x: numpy.percentile(x, q=20),
                                                                 'age_ad_p25_msa': lambda x: numpy.percentile(x, q=25),
                                                                 'age_ad_p30_msa': lambda x: numpy.percentile(x, q=30),
                                                                 'age_ad_p35_msa': lambda x: numpy.percentile(x, q=35),
                                                                 'age_ad_p40_msa': lambda x: numpy.percentile(x, q=40),
                                                                 'age_ad_p45_msa': lambda x: numpy.percentile(x, q=45),
                                                                 'age_ad_p50_msa': lambda x: numpy.percentile(x, q=50),
                                                                 'age_ad_p55_msa': lambda x: numpy.percentile(x, q=55),
                                                                 'age_ad_p60_msa': lambda x: numpy.percentile(x, q=60),
                                                                 'age_ad_p65_msa': lambda x: numpy.percentile(x, q=65),
                                                                 'age_ad_p70_msa': lambda x: numpy.percentile(x, q=70),
                                                                 'age_ad_p75_msa': lambda x: numpy.percentile(x, q=75),
                                                                 'age_ad_p80_msa': lambda x: numpy.percentile(x, q=80),
                                                                 'age_ad_p85_msa': lambda x: numpy.percentile(x, q=85),
                                                                 'age_ad_p90_msa': lambda x: numpy.percentile(x, q=90),
                                                                 'age_ad_p95_msa': lambda x: numpy.percentile(x, q=95)})

    def get_rates(self):
        """
        Creates a data frame with a new rates column from the rates column with comma delimted fields
        :return:
        """
        # Get only rates and msa
        df = self.dataframe[['rate', 'msa_name']].dropna(0)

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
        return self.dataframe[['age', 'msa_name']].dropna(0)



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
        df['post_date'] =  pandas.to_datetime(df['post_date'], format='%Y-%m-%d')

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

        value = pandas.Timestamp(datetime.date(year, month, day))
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