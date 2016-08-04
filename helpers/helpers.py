def _is_float(x):
    """
    Checks if a number is valid
    :param x: Test number
    :return: True if valid, False otherwise
    """
    try:
        float(x)
    except ValueError:
        return False
    return True


def _strip_currency_chars(x):
    """
    Strips common characters that mean 'dollar' from the rate
    :param x:
    :return: Rate without common characters
    """
    replace_characters = ['$',
                          'roses',
                          'rose',
                          'bucks',
                          'kisses',
                          'kiss',
                          'dollars',
                          'dollar',
                          'dlr']
    for char in replace_characters:
        x = x.replace(char, '')

    return x.strip()


def mean_hourly_rate(rates):
    """
    Takes a list of comma-delimited rate strings and calculates the mean hourly rate
    :param list rates: Comma delimited rate from rate file
    :returns: `np.float` -- Hourly rate
    """
    import numpy as np

    if not isinstance(rates, list) or len(rates) == 0:
        return None

    calculated_rates = []
    hour_rates = []
    for r in rates:
        # Split the rate by comma leaving the price and the unit.
        rate_info = r.split(',')
        price = rate_info[0]
        # Remove any currency characters from the price
        price = _strip_currency_chars(price)

        # Make sure the price is a number
        if _is_float(price):
            unit_info = rate_info[1].split(' ')
            unit = unit_info[0]
            duration = unit_info[1]

            if duration == 'HOUR':
                hour_rates.append(float(price))
            elif len(hour_rates) < 1:
                if duration == 'MINS':
                    calculated_rates.append((60 / float(unit)) * float(price))
                elif duration == 'HOURS':
                    calculated_rates.append(float(price) / float(unit))

    if len(hour_rates) > 0:
        return np.mean(hour_rates)

    return np.mean(calculated_rates)


def mean_hourly_rate_df(df, grouping_col='_id'):
    """
    Calculate the mean hourly rate for some grouping column
    :param pandas.DataFrame df:
    :param str grouping_col:
    :return: `pandas.DataFrame` --
    """
    per_hour_df = df.groupby(grouping_col)['rate'].\
        apply(lambda x: mean_hourly_rate(list(x))).\
        dropna().\
        reset_index()
    per_hour_df.columns = [grouping_col, 'rate_per_hour']
    return per_hour_df.dropna()
