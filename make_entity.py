import pandas
import numpy

class MakeEntity:
    def __init__(self, dataframe, entity):
        self.dataframe = dataframe
        self.entity = entity

    def get_entity_features(self):
        # First we will calculate the rates, so lets drop all the NaN
        rate_dataframe = self.dataframe.dropna(subset=['rate'])

        # Calculate the rates by hour and delete the old rate column. Then drop any remaining NaN
        rate_dataframe['rate_per_hour'] = rate_dataframe['rate'].apply(lambda x: self.calculate_rate(x))
        rate_dataframe = rate_dataframe.drop('rate', 1)
        rate_dataframe = rate_dataframe.dropna(subset=['rate_per_hour'])

        # Now get the stats we want for rate
        rate_dataframe = self.calculate_entity_rate_features(rate_dataframe)

        # Get a count of the ads
        dataframe = pandas.value_counts(self.dataframe[self.entity]).to_frame()
        # Reset the index so we can use the entity column
        dataframe.reset_index(level=0, inplace=True)

        # Get the unique locations
        dataframe['unique_cities'] = dataframe['index'].apply(lambda x: self.get_unique_cities(x))
        dataframe['unique_states'] = dataframe['index'].apply(lambda x: self.get_unique_states(x))

        # Now give the columns the proper names as they have changed
        dataframe.columns = ['phone', 'phone_count', 'unique_cities', 'unique_states']

        # Reset the index on our rate dataframe and rename the columns
        rate_dataframe.reset_index(level=0, inplace=True)
        rate_dataframe.columns = ['phone', 'rate_count', 'rate_mean', 'rate_std', 'rate_median']

        # Lastly merge the two dataframes
        dataframe = dataframe.merge(rate_dataframe, how='outer')

        # Save this code as we may use it later
        """dataframe['incall_count'] = dataframe['index'].apply(lambda x: self.get_incall_count(x))
        dataframe['outcall_count'] = dataframe['index'].apply(lambda x: self.get_outcall_count(x))"""

        return dataframe

    def calculate_entity_rate_features(self, df_entity_rates):
        """

        :param df_msa_rates:
        :return: MSA rate features
        """
        return df_entity_rates.groupby(self.entity)['rate_per_hour'].aggregate({'rate_count': len,
                                                                            'rate_mean': numpy.mean,
                                                                            'rate_std': numpy.std,
                                                                            'rate_median': lambda x: numpy.percentile(x, q=50)})

    def get_unique_cities(self, value):
        return len(pandas.value_counts(self.dataframe.loc[self.dataframe[self.entity]==value]['city']))

    def get_unique_states(self, value):
        return len(pandas.value_counts(self.dataframe.loc[self.dataframe[self.entity]==value]['state']))

    # Save this code as we may use it later
    """def get_incall_count(self, phone):
        return pandas.value_counts(self.dataframe.loc[self.dataframe['phone']==phone]['service'].str.contains('incall')).sum()
        #TODO only evaluate true

    def get_outcall_count(self, phone):
        return pandas.value_counts(self.dataframe.loc[self.dataframe['phone']==phone]['service'].str.contains('outcall')).sum()"""

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