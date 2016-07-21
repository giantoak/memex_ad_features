import pandas
import numpy

class MakeEntity:
    def __init__(self, dataframe, entity):
        self.dataframe = dataframe
        self.entity = entity

    def get_entity_features(self):
        # Get the rates
        entity_rates = self.get_rates()
        # Get the rate features
        df_entity_rate = self.calculate_entity_rate_features(entity_rates)

        # Get a count of the ads
        dataframe = pandas.value_counts(self.dataframe['phone']).to_frame()
        # Reset the index so we can use the phone column
        dataframe.reset_index(level=0, inplace=True)

        # Get the unique locations
        dataframe['unique_locations'] = dataframe['index'].apply(lambda x: self.get_unique_location(x))
        dataframe['incall_count'] = dataframe['index'].apply(lambda x: self.get_incall_count(x))
        dataframe['outcall_count'] = dataframe['index'].apply(lambda x: self.get_outcall_count(x))

    def calculate_entity_rate_features(self, df_entity_rates):
        """

        :param df_msa_rates:
        :return: MSA rate features
        """
        return df_entity_rates.groupby(self.entity)['rate_per_hour'].aggregate({'rate_count': len,
                                                                            'rate_mean': numpy.mean,
                                                                            'rate_std': numpy.std,
                                                                            'rate_median': lambda x: numpy.percentile(x, q=5)})

    def get_unique_location(self, phone):
        return pandas.value_counts(self.dataframe.loc[self.dataframe['phone']==phone]['msa_name']).sum()

    def get_incall_count(self, phone):
        return pandas.value_counts(self.dataframe.loc[self.dataframe['phone']==phone]['service'].str.contains('incall')).sum()
        #TODO only evaluate true

    def get_outcall_count(self, phone):
        return pandas.value_counts(self.dataframe.loc[self.dataframe['phone']==phone]['service'].str.contains('outcall')).sum()

    def get_rates(self):
        """
        Creates a data frame with a new rates column from the rates column with comma delimted fields
        :return:
        """
        # Get only rates and msa
        df = self.dataframe[['rate', self.entity]].dropna(0)

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