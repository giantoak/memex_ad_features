import pandas as pd
import numpy as np


class MakeEntity:
    def __init__(self, df, entity):
        self.df = df
        self.entity = entity

    def get_entity_features(self):
        # First we will calculate the rates, so lets drop all the NaN
        rate_df = self.df.dropna(subset=['rate'])

        # Calculate the rates by hour and delete the old rate column.
        # Then drop any remaining NaN

        rate_df['rate_per_hour'] = rate_df['rate'].apply(lambda x:
                                                         self.calculate_rate(x))
        rate_df = rate_df.drop('rate', 1)
        rate_df = rate_df.dropna(subset=['rate_per_hour'])

        # Now get the stats we want for rate
        rate_df = self.calculate_entity_rate_features(rate_df)

        # Get a count of the ads
        df = pd.value_counts(self.df[self.entity]).to_frame()
        # Reset the index so we can use the entity column
        df.reset_index(level=0, inplace=True)

        # Get the unique locations
        df['unique_cities'] = df['index'].apply(lambda x: self.get_unique_cities(x))
        df['unique_states'] = df['index'].apply(lambda x: self.get_unique_states(x))

        # Now give the columns the proper names as they have changed
        df.columns = ['phone', 'phone_count', 'unique_cities', 'unique_states']

        # Reset the index on our rate dataframe and rename the columns
        rate_df.reset_index(level=0, inplace=True)
        rate_df.columns = ['phone', 'rate_count', 'rate_mean', 'rate_std', 'rate_median']

        # Lastly merge the two dataframes
        df = df.merge(rate_df, how='outer')

        # Save this code as we may use it later
        """df['incall_count'] = df['index'].apply(lambda x: self.get_incall_count(x))
        df['outcall_count'] = df['index'].apply(lambda x: self.get_outcall_count(x))"""

        return df

    def calculate_entity_rate_features(self, df_entity_rates):
        """

        :param df_msa_rates:
        :return: MSA rate features
        """
        return df_entity_rates.groupby(self.entity)['rate_per_hour'].aggregate({'rate_count': len,
                                                                                'rate_mean': np.mean,
                                                                                'rate_std': np.std,
                                                                                'rate_median': lambda x: np.percentile(
                                                                                    x, q=50)})

    def get_unique_cities(self, value):
        return len(pd.value_counts(self.df.loc[self.df[self.entity] == value]['city']))

    def get_unique_states(self, value):
        return len(pd.value_counts(self.df.loc[self.df[self.entity] == value]['state']))

    # Save this code as we may use it later
    """def get_incall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('incall')).sum()
        #TODO only evaluate true

    def get_outcall_count(self, phone):
        return pd.value_counts(self.df.loc[self.df['phone']==phone]['service'].str.contains('outcall')).sum()"""

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
