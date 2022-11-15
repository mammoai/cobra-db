import pytest

from cobra_db.dataset_mod import DatasetMod


@pytest.fixture
def tags_dict():
    case = {
        "00400260": {
            "vr": "SQ",
            "Value": [
                {
                    "00080100": {"vr": "SH", "Value": ["93800"]},
                    "00080102": {"vr": "SH", "Value": ["SECTRA RIS"]},
                }
            ],
        },
        "00080070": {"vr": "LO"},
        "00100020": {"vr": "LO", "Value": ["TESTTESTTEST"]},
        "60100020": {"vr": "LO", "Value": ["TESTTEST"]},
        "00280037": {"vr": "LO", "Value": ["TESTTEST"]},
        "12341234": {"vr": "LO", "Value": ["TESTTEST"]},
    }
    return case


@pytest.fixture
def keywords_dict():
    case = {
        "PerformedProtocolCodeSequence": {
            "vr": "SQ",
            "Value": [
                {
                    "CodeValue": {"vr": "SH", "Value": ["93800"]},
                    "CodingSchemeDesignator": {"vr": "SH", "Value": ["SECTRA RIS"]},
                }
            ],
        },
        "Manufacturer": {"vr": "LO"},
        "PatientID": {"vr": "LO", "Value": ["TESTTESTTEST"]},
        "60100020": {"vr": "LO", "Value": ["TESTTEST"]},
        "00280037": {"vr": "LO", "Value": ["TESTTEST"]},
        "12341234": {"vr": "LO", "Value": ["TESTTEST"]},
    }
    return case


def test_tags_to_keywords(tags_dict, keywords_dict):
    assert DatasetMod.tags_to_keywords(tags_dict) == keywords_dict


def test_keywords_to_tags(keywords_dict, tags_dict):
    assert DatasetMod.keywords_to_tags(keywords_dict) == tags_dict


def test_tag_for_keyword_wrapper():
    assert DatasetMod.tag_for_keyword_wrapper("PatientID") == "00100020"
