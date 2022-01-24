import pytest
from UUIDParser import UUIDParser


def test_convert_to_uuid_input_success():
    parser = UUIDParser()
    assert parser.convert_to_uuid(
        '1d25d1f5-8b05-41ad-8c2e-912ea52d8103') == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    assert parser.convert_to_uuid(
        '1d25d1f58b0541ad8c2e912ea52d8103') == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    assert parser.convert_to_uuid(
        '1D25D1F58B05-41AD-8C2E-912EA52D8103') == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    assert parser.convert_to_uuid(
        '1D25D1F58B0541AD8C2E912EA52D8103') == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'


def test_convert_to_uuid_without_seperators_success():
    parser = UUIDParser()
    assert parser.convert_to_uuid_without_seperators(
        '1d25d1f5-8b05-41ad-8c2e-912ea52d8103') == '1d25d1f58b0541ad8c2e912ea52d8103'
    assert parser.convert_to_uuid_without_seperators(
        '1d25d1f5-8b05-41ad8c2e912ea52d8103') == '1d25d1f58b0541ad8c2e912ea52d8103'


def test_convert_to_uppercase_uuid_success():
    parser = UUIDParser()
    assert parser.convert_to_uppercase_uuid(
        '1d25d1f5-8b05-41ad-8c2e-912ea52d8103') == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'
    assert parser.convert_to_uppercase_uuid(
        '1d25d1f58b05-41ad-8c2e-912ea52d8103') == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'
    assert parser.convert_to_uppercase_uuid(
        '1d25d1f58b0541ad8c2e912ea52d8103') == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'


def test_convert_to_uppercase_uuid_without_seperators_success():
    parser = UUIDParser()
    assert parser.convert_to_uppercase_uuid_without_seperators(
        '1d25d1f5-8b05-41ad-8c2e-912ea52d8103') == '1D25D1F58B0541AD8C2E912EA52D8103'
    assert parser.convert_to_uppercase_uuid_without_seperators(
        '1d25d1f58b05-41ad-8c2e-912ea52d8103') == '1D25D1F58B0541AD8C2E912EA52D8103'
    assert parser.convert_to_uppercase_uuid_without_seperators(
        '1d25d1f58b0541ad8c2e912ea52d8103') == '1D25D1F58B0541AD8C2E912EA52D8103'
