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


def mean_hourly_rate(rate):
    """
    Takes a list of comma-delimited rate strings and calculates the mean hourly rate
    :param str rate: Comma delimited rate from rate file
    :returns: `np.float` -- Hourly rate
    """
    import numpy as np

    if not isinstance(rate, list) or len(rate) == 0:
        return None

    calculated_rates = []
    for r in rate:
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

            if duration == 'MINS':
                calculated_rates.append((60 / float(unit)) * float(price))
            elif duration == 'HOUR':
                # If it's an hour stop calculating and return it
                return float(price)
            elif duration == 'HOURS':
                calculated_rates.append(float(price) / float(unit))

    return np.mean(calculated_rates)
