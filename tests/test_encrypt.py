import pytest

from cobra_db.encrypt import Hasher, PersonalNumberValidator
from cobra_db.enums import PersonalNumberValidity as V


@pytest.fixture
def secret_salt():
    return "ThisIsAVerySectretSalt"


@pytest.mark.parametrize(
    "pnr,validity",
    (
        (
            "811228-9874",
            V.CHECKSUM | V.DATE,
        ),  # one extra character and wrong date format
        ("8112289873", V.DATE | V.CHARS),  # wrong checksum and lenght
        ("8112289874", V.VALID - V.LENGTH),  # valid pnr but missing '19'
        ("198112289874", V.VALID),
        ("81-12-288984", V.DATE),
        ("123456789101", V.LENGTH | V.CHARS),
        ("19000101-ABCD", V.LENGTH | V.DATE),
    ),
)
def test_valid_personal_number(pnr, validity):
    assert PersonalNumberValidator.validate(pnr) == validity


@pytest.mark.parametrize(
    "pnr,hashed,validity",
    (
        (
            "197005061111",
            "22c678921a5dbefb874c5cdeaa07f53e4b803ba804fc5ec6e80c4e578f1862eb",
            V.DATE | V.LENGTH | V.CHARS,
        ),
        (
            "19811228-9874",
            "ac9c8d9262574bca178968b3dd2130afdb282be377bd86aea35bc8e838e982e8",
            V.VALID,
        ),
    ),
)
def test_hash_personal_number(pnr, hashed, validity, secret_salt):
    hasher = Hasher(secret_salt)
    hashed_pnr, validity_pnr = hasher.hash_personal_number(pnr)
    assert hashed_pnr == hashed
    assert validity_pnr == validity
