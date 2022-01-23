import pytest
from UUIDParser import UUIDParser

def test_convert_to_uuid_input_standard_input_uuid_success():
    uuid = '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    parser = UUIDParser()
    result = parser.convert_to_uuid(uuid)
    assert result == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'

def test_convert_to_uuid_input_inconsistent_seperators_success():
    uuid = '1d25d1f58b05-41ad-8c2e-912ea52d8103'
    uuid_2 = '1d25d1f58b0541ad8c2e912ea52d8103'
    parser = UUIDParser()
    assert parser.convert_to_uuid(uuid) == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    assert parser.convert_to_uuid(uuid_2) == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'

def test_convert_to_uuid_input_capital_inconsistent_seperators_success():
    uuid = '1D25D1F58B05-41AD-8C2E-912EA52D8103'
    uuid_2 = '1D25D1F58B0541AD8C2E912EA52D8103'
    parser = UUIDParser()
    assert parser.convert_to_uuid(uuid) == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    assert parser.convert_to_uuid(uuid_2) == '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'

def test_convert_to_uuid_without_seperators_input_uuid_success():
    uuid = '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    parser = UUIDParser()
    result = parser.convert_to_uuid_without_seperators(uuid)
    assert result == '1d25d1f58b0541ad8c2e912ea52d8103'

def test_convert_to_uuid_without_seperators_input_inconsistent_seperators_success():
    uuid = '1d25d1f5-8b0541ad-8c2e912ea52d8103'
    parser = UUIDParser()
    result = parser.convert_to_uuid_without_seperators(uuid)
    assert result == '1d25d1f58b0541ad8c2e912ea52d8103'

def test_convert_to_uppercase_uuid_input_standard_input_uuid_success():
    uuid = '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    parser = UUIDParser()
    result = parser.convert_to_uppercase_uuid(uuid)
    assert result == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'

def test_convert_to_uppercase_uuid_input_inconsistent_seperators_success():
    uuid = '1d25d1f58b05-41ad-8c2e-912ea52d8103'
    uuid_2 = '1d25d1f58b0541ad8c2e912ea52d8103'
    parser = UUIDParser()
    assert parser.convert_to_uppercase_uuid(uuid) == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'
    assert parser.convert_to_uppercase_uuid(uuid_2) == '1D25D1F5-8B05-41AD-8C2E-912EA52D8103'

def test_convert_to_uppercase_uuid_without_seperators_input_standard_input_uuid_success():
    uuid = '1d25d1f5-8b05-41ad-8c2e-912ea52d8103'
    parser = UUIDParser()
    result = parser.convert_to_uppercase_uuid_without_seperators(uuid)
    assert result == '1D25D1F58B0541AD8C2E912EA52D8103'

def testconvert_to_uppercase_uuid_without_seperators_input_inconsistent_seperators_success():
    uuid = '1d25d1f58b05-41ad-8c2e-912ea52d8103'
    uuid_2 = '1d25d1f58b0541ad8c2e912ea52d8103'
    parser = UUIDParser()
    assert parser.convert_to_uppercase_uuid_without_seperators(uuid) == '1D25D1F58B0541AD8C2E912EA52D8103'
    assert parser.convert_to_uppercase_uuid_without_seperators(uuid_2) == '1D25D1F58B0541AD8C2E912EA52D8103'