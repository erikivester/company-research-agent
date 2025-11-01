import pytest

from backend.utils.utils import company_name


def test_company_present():
    state = {'company': 'Acme Corp'}
    assert company_name(state) == 'Acme Corp'


def test_company_empty_inferred_present():
    state = {'company': '', 'inferred_company': 'Inferred Co'}
    assert company_name(state) == 'Inferred Co'


def test_company_none_inferred_none():
    state = {'company': None, 'inferred_company': None}
    assert company_name(state) == 'Unknown Company'


def test_state_not_dict():
    assert company_name(None) == 'Unknown Company'


def test_company_whitespace():
    state = {'company': '   ', 'inferred_company': '  '}
    assert company_name(state) == 'Unknown Company'
