import pydicom
import pytest

from cobra_db.deid import Deider, base_recipe_path


@pytest.fixture
def real_ds():
    return pydicom.Dataset.from_json(
        {
            "SpecificCharacterSet": {"vr": "CS", "Value": ["ISO_IR 100"]},
            "ImageType": {"vr": "CS", "Value": ["DERIVED", "PRIMARY"]},
            "SOPClassUID": {"vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.1.2"]},
            "StudyDate": {"vr": "DA", "Value": ["20220627"]},
            "SeriesDate": {"vr": "DA", "Value": ["20220627"]},
            "AcquisitionDate": {"vr": "DA", "Value": ["20220627"]},
            "ContentDate": {"vr": "DA", "Value": ["20220627"]},
            "StudyTime": {"vr": "TM", "Value": ["080803"]},
            "ContentTime": {"vr": "TM", "Value": ["080808.202000"]},
            "PatientName": {"vr": "PN", "Value": [{"Alphabetic": "Maria^Doe"}]},
            "PatientID": {"vr": "LO", "Value": ["1234567890"]},
            "PatientBirthDate": {"vr": "DA", "Value": ["19900606"]},
            "Modality": {"vr": "CS", "Value": ["MG"]},
            "PatientSex": {"vr": "CS", "Value": ["F"]},
            "PatientAge": {"vr": "AS", "Value": ["032Y"]},
            "StudyID": {"vr": "SH", "Value": ["mammogram87654"]},
        }
    )


@pytest.fixture
def expected_ds():
    return pydicom.Dataset.from_json(
        {
            "00080005": {"Value": ["ISO_IR 100"], "vr": "CS"},
            "00080008": {"Value": ["DERIVED", "PRIMARY"], "vr": "CS"},
            "00080016": {"Value": ["1.2.840.10008.5.1.4.1.1.1.2"], "vr": "UI"},
            "00080020": {"Value": ["20220627"], "vr": "DA"},
            "00080021": {"Value": ["20220627"], "vr": "DA"},
            "00080022": {"Value": ["20220627"], "vr": "DA"},
            "00080023": {"Value": ["20220627"], "vr": "DA"},
            "00080030": {"Value": ["080803"], "vr": "TM"},
            "00080033": {"Value": ["080808.202000"], "vr": "TM"},
            "00080060": {"Value": ["MG"], "vr": "CS"},
            "00100010": {"Value": [{"Alphabetic": "Female 030Y MG"}], "vr": "PN"},
            "00100020": {
                "Value": [
                    "0806640c30f711630aee80238489022878c57f2507cf873ffbac6ff297488277"
                ],
                "vr": "LO",
            },
            "00100030": {"Value": ["19900601"], "vr": "DA"},
            "00100040": {"Value": ["F"], "vr": "CS"},
            "00101010": {"Value": ["030Y"], "vr": "AS"},
            "00120062": {"Value": ["Yes"], "vr": "CS"},
            "00120063": {"Value": ["vaib_deid_v1.0.1"], "vr": "LO"},
            "00200010": {
                "Value": [
                    "fde000d590d076679bc13cb5ef1621ce8437f3e0512ea96d0404235f06f971d5"
                ],
                "vr": "SH",
            },
        }
    )


def test_deid_dataset(real_ds: pydicom.Dataset, expected_ds: pydicom.Dataset):

    deider = Deider("AVerySecretSalt", base_recipe_path)
    pseudon_ds = deider.pseudonymize(real_ds)
    assert pseudon_ds.to_json() == expected_ds.to_json()
