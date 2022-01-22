import re


def find_value_between_first_tree_forward_slashes(URL):
    """
    returns the value between the 1st and 2nd slash if its not 'lp'
    returns the value between the 2nd and 3rd slash if value between the 1st and 2nd is lp

    :param URL: a url containing tree forward slashes.
    :type URL: str

    for example:
        'try.newfaithnetwork.com/en/en-au/?fbclid=iwar returns: en
        'try.newfaithnetwork.com/lp/en-au/?fbclid=iwar returns: en-au
    """
    get_all_chars_between_first_tree_brackets = r"\/(.*?)/(.*?)/"
    matches = re.finditer(get_all_chars_between_first_tree_brackets, URL)
    if matches is not None:
        for matchNum, match in enumerate(matches, start=1):
            chars_between_1st_2nd_brackets = match.group(1)
            chars_between_2nd_3rd_brackets = match.group(2)
            if matchNum == 1 and chars_between_1st_2nd_brackets is not None and chars_between_1st_2nd_brackets != 'lp':
                return chars_between_1st_2nd_brackets
            elif matchNum == 1 and chars_between_2nd_3rd_brackets is not None:
                return chars_between_2nd_3rd_brackets
    else:
        return None


def find_value_between_first_two_forward_slashes(string):
    """
    returns the value between the 1st and 2nd slash when there are atleast 3 slashed else return None

    :param URL: a url containing tree forward slashes.
    :type URL: str

    for example:
        try.newfaithnetwork.com/lp/?fbclid=iwa returns: None
        help.newfaithnetwork.com/en/support/solutions/articles/ returns: en
    """

    regex = r"\/(.*?)/"
    matches = re.finditer(regex, string)
    if matches is not None:
        for matchNum, match in enumerate(matches, start=1):
            if matchNum == 1 and match.group(1) is not None:
                return match.group(1)
    else:
        return None
