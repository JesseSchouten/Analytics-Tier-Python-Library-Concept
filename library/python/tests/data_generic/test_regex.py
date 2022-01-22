from data_generic.data_regex import *


def test_find_values_with_lp_and_tree_brackets():
    result = find_value_between_first_tree_forward_slashes(
        'try.newfaithnetwork.com/lp/en-au/?fbclid=iwar0kso7al8i7elyruuxkydw7gvn')
    assert result == 'en-au'


def test_find_values_with_en_and_tree_brackets():
    result = find_value_between_first_tree_forward_slashes(
        'try.newfaithnetwork.com/en/en-au/?fbclid=iwar0kso7al8i7elyruuxkydw7gvn')
    assert result == 'en'


def test_find_values_without_lp_and_two_brackets():
    result = find_value_between_first_tree_forward_slashes(
        'try.newfaithnetwork.com/lp/?fbclid=iwar0kso7al8i7elyruuxkydw7gvn')
    assert result == None


def test_tree_slashes():
    result = find_value_between_first_tree_forward_slashes(
        '///')
    assert result == ''


def test_four_slashes():
    result = find_value_between_first_tree_forward_slashes(
        '/1234///')
    assert result == '1234'


def test_tree_both_with_values_slashes():
    result = find_value_between_first_tree_forward_slashes(
        '/1234/4321/')
    assert result == '1234'


def test_two_slashes():
    result = find_value_between_first_tree_forward_slashes(
        '//')
    assert result == None


def test_tree_slashes_and_special_chars():
    result = find_value_between_first_two_forward_slashes(
        '/here-is-a-long-line-with-special-chars-!@#$%^&*()_+=//')
    assert result == 'here-is-a-long-line-with-special-chars-!@#$%^&*()_+='


def test_find_value_with_two_brackets():
    result = find_value_between_first_two_forward_slashes(
        'help.newfaithnetwork.com/en/support/solutions/articles/4')
    assert result == 'en'
