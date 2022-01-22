from data_generic.data_regex import find_value_between_first_tree_forward_slashes, find_value_between_first_two_forward_slashes


def determine_country(URL, iso_country):
    """
    Tries to determinces country based on URL and falls back to iso country code if it does not succeed.
    First looks at the value between the first 3 forward slashes.
    When URL is inconclusive ISO code is used.

    Countries that we do not service default to NL

    On error return nan

    :param URL: a url containing tree forward slashes.
    :type URL: str
    :param iso_country: an 2 char ISO country code.
    :type URL: str

    for examples:
        determine_country(
        'newfaithnetwork.com/lp/en-gb/payment', 'gb') returns GB
        determine_country(
        'help.newfaithnetwork.com/en/support/solutions/articles/4', 'gb') returns GB
        determine_country(
        'newfaithnetwork.com/nl/discover', 'be')

    see library/python/tests/data_generic/test_country.py for more examples
    """
    if not isinstance(URL, str):
        print('exit page path: ' + URL + ' is not a string')
        return determine_country_in_string(iso_country)

    match = find_value_between_first_tree_forward_slashes(URL)

    if match is not None and match not in 'en':
        return determine_country_in_string(match)

    match = find_value_between_first_two_forward_slashes(URL)

    if match is not None and match not in 'en' and (match not in 'nl' and iso_country not in 'BE'):
        return determine_country_in_string(match)

    return determine_country_in_string(iso_country)


def determine_country_in_string(string):
    """
    Tries to determine channel country.
    Returns capitalized 2 cahr ISO country code.
    Defaults to NL for countries without channel

    :param string: a string containing country codes
    :type string: str
    """
    if not isinstance(string, str):
        print(string + ' is not a string setting country as nan')
        return 'nan'
    string = string.lower()
    if 'nl' in string:
        return 'NL'
    elif 'be' in string:
        return 'BE'
    elif 'se' in string or 'sv' in string:
        return 'SE'
    elif 'no' in string or 'nn' in string or 'nb' in string:
        return 'NO'
    elif 'nz' in string:
        return 'NZ'
    elif 'gb' in string:
        return 'GB'
    elif 'au' in string:
        return 'AU'
    elif 'ie' in string:
        return 'IE'
    elif 'de' in string:
        return 'DE'
    else:
        return 'NL'
