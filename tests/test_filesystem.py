import os

import pytest
from pydicom import read_file

from cobra_db.deid import Deider, default_recipe_path
from cobra_db.filesystem import get_instance_path

DICOM_PATH = "dicom_data/6774825273/09-11-2013-NA-XR CHEST AP PORTABLE for Scott Kaufman\
-73078/1.000000-NA-91318/1-1.dcm"


@pytest.fixture
def real_ds():
    pwd = os.path.dirname(__file__)
    data_path = os.path.join(pwd, DICOM_PATH)
    return read_file(data_path, stop_before_pixels=True)


@pytest.fixture
def deid_ds(real_ds):
    deider = Deider("VerySecretSalt", default_recipe_path)
    return deider.pseudonymize(real_ds)


def test_get_instance_path(real_ds, deid_ds):
    with pytest.raises(AssertionError) as exc_info:
        get_instance_path(real_ds)
    assert exc_info.value.args[0] == "Length of patient hash is incorrect"
    expected = "e18/2ce/29b495df648d52d1eb/study_20131109/series_CT_162723_UNK/1.dcm"
    assert get_instance_path(deid_ds) == expected


def test_missing_PatientID():
    ds_missing_patient_id = "dicom_data/missing_patient_id.dcm"
    pwd = os.path.dirname(__file__)
    data_path = os.path.join(pwd, ds_missing_patient_id)
    ds = read_file(data_path, stop_before_pixels=True)
    tag = ds.data_element("PatientID").tag
    del ds[tag]
    deider = Deider("VerySecretSalt", default_recipe_path)
    expected_path = get_instance_path(deider.pseudonymize(ds))
    assert expected_path == "UNK_PatientID/study_20040604/series_CR_054409_UNK/1001.dcm"
