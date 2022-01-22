
from data_generic.country import determine_country


def test_determine_country_given_url_lp_en_au_iso_nl_then_AU():
    result = determine_country(
        'try.newfaithnetwork.com/lp/en-au/?fbclid=iwar0kso7al8i7elyruuxkydw7gvn', 'nl')
    assert result == 'AU'


def test_determine_country_given_url_lp_en_gb_iso_gb_then_GB():
    result = determine_country(
        'newfaithnetwork.com/lp/en-gb/payment', 'gb')
    assert result == 'GB'


def test_determine_country_given_url_lp_en_nz_iso_nz_then_NZ():
    result = determine_country(
        'newfaithnetwork.com/lp/en-nz/payment-finish', 'nz')
    assert result == 'NZ'


def test_determine_countr_given_url_lp_en_nz_iso_nz_then_NZ():
    result = determine_country(
        'newfaithnetwork.com/lp/nb-no/reconnect-vouchercode/?vgo_ee=79xgeozgrwf+p5/e0apxtkbadrvsmjbjpsompfr7qrk=', 'no')
    assert result == 'NO'


def test_determine_country_given_url_lp_en_nz_iso_nz_then_NZ():
    result = determine_country(
        'try.newfaithnetwork.com/lp/nl-nl/perspagina/', 'nl')
    assert result == 'NL'


def test_determine_country_given_url_en_iso_gb_then_GB():
    result = determine_country(
        'help.newfaithnetwork.com/en/support/solutions/articles/4', 'gb')
    assert result == 'GB'


def test_determine_countr_given_given_url_lp_nb_no_iso_no_then_NO():
    result = determine_country(
        'try.newfaithnetwork.com/lp/nb-no/?fbclid=iwar2hdkmftfjdjr5_cki5rtlamzs733be7dg', 'no')
    assert result == 'NO'


def test_determine_countr_given_url_nl_iso_nl_then_NL():
    result = determine_country(
        'newfaithnetwork.com/nl/discover', 'nl')
    assert result == 'NL'


def test_determine_country_given_url_nl_iso_be_then_BE():
    result = determine_country(
        'newfaithnetwork.com/nl/discover', 'be')
    assert result == 'BE'
