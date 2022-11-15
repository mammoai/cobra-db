import os
from datetime import datetime

import pydicom
import pytest
from bson import ObjectId
from pydicom.data import get_testdata_file

from cobra_db.model import (
    FileSource,
    ImageMetadata,
    Patient,
    RadiologicalSeries,
    RadiologicalStudy,
)

mongo_time_format = "%Y-%m-%dT%H:%M:%S.%fZ"


def test_file_source():
    """Requires that the file exists"""
    mount_paths = {"TestFolder1": os.path.join(os.path.dirname(__file__), "folder1")}
    full_filepath = os.path.join(
        os.path.dirname(__file__), "folder1/folder2/file_source_example.txt"
    )

    f = FileSource.from_mount_paths(full_filepath, mount_paths)
    file_source = FileSource(
        filename="file_source_example.txt",
        rel_path="folder2/file_source_example.txt",
        drive_name="TestFolder1",
    )
    assert f == file_source

    filepath = file_source.get_local_filepath(mount_paths)
    assert filepath == full_filepath

    with open(filepath) as file:
        line = next(file)
    assert line == "It works!\n"


@pytest.fixture
def patient_dict():
    return {
        "_id": ObjectId("622b465c4b329eb8c3db86dd"),
        "_metadata": {"model_version": "0.1.2", "created": datetime(2022, 3, 11)},
        "anon_id": "6938365a64316d38545862574a7446647976526a49673d3d",
        "birth_date": datetime(year=1900, month=1, day=1),
        "hidden": False,
    }


def test_patient_from_dict_to_dict(patient_dict):
    p = Patient.from_dict(patient_dict)
    assert p.to_dict() == patient_dict


@pytest.fixture
def radiological_study_dict():
    return {
        "_id": ObjectId("622b4658d28b1846f9db887e"),
        "study_uid": ["1.1.2.3.4.5"],
        "modality": ["MR"],
        "date": datetime.strptime("2000-02-02T20:20:20.000Z", mongo_time_format),
        "description": "MR Scan",
        "accession_number": "1111111111",
        "patient_age": 60,
        "dicom_tags": {
            "AccessionNumber": {"vr": "SH", "Value": ["1111111111"]},
            "AdditionalPatientHistory": {"vr": "LT"},
            "BodyPartExamined": {"vr": "CS", "Value": ["MR BROST"]},
            "Laterality": {"vr": "CS"},
            "Modality": {"vr": "CS", "Value": ["MR"]},
            # Many more dicom tags go in here ...
        },
        "series_count": 41,
        "_metadata": {
            "model_version": "0.1.2",
            "created": datetime.strptime("2022-03-11T12:53:44.012Z", mongo_time_format),
            "modified": datetime.strptime(
                "2022-03-11T12:53:50.811Z", mongo_time_format
            ),
        },
        "patient_id": ObjectId("622b465ef772b010a0db8799"),
    }


def test_radiological_study_from_dict_to_dict(radiological_study_dict):
    s = RadiologicalStudy.from_dict(radiological_study_dict)
    assert s.to_dict() == radiological_study_dict


def test_radiological_study_get_tag(radiological_study_dict):
    s = RadiologicalStudy.from_dict(radiological_study_dict)
    assert s.get_tag("Modality") == "MR"
    assert s.get_tag("Laterality") is None


@pytest.fixture
def radiological_series_dict():
    return {
        "_id": ObjectId("622b3a046096e5d336db8706"),
        "series_uid": "1.1.111.11.11.1.1111111111.1111111111.111111111.11111111",
        "date": datetime.strptime("2018-02-20T13:45:10.190Z", mongo_time_format),
        "protocol_name": "eTHRIVE_Tra 1-6 dyn",
        "image_count": 170,
        "image_shape": [720, 720],
        "description": "eTHRIVE_Tra 1-6 dyn",
        "dicom_tags": {
            "AccessionNumber": {"vr": "SH", "Value": ["SEKBF00011190317"]},
            "AcquisitionDate": {"vr": "DA", "Value": ["20180619"]},
            "AcquisitionDuration": {"vr": "FD", "Value": [426.7959899902344]},
            "AcquisitionMatrix": {"vr": "US", "Value": [0, 377, 377, 0]},
            "AcquisitionNumber": {"vr": "IS", "Value": [2]},
            "AcquisitionTime": {"vr": "TM", "Value": ["145125.32"]},
            # Many other dicom_tags go in here ...
        },
        "_metadata": {
            "model_version": "0.1.3",
            "created": datetime.strptime("2022-03-11T12:01:08.416Z", mongo_time_format),
            "modified": datetime.strptime(
                "2022-03-11T12:53:13.680Z", mongo_time_format
            ),
        },
        "study_id": ObjectId("622b46395f0df14e11db8740"),
    }


def test_radiological_series_from_dict_to_dict(radiological_series_dict):
    s = RadiologicalSeries.from_dict(radiological_series_dict)
    assert s.to_dict() == radiological_series_dict


def test_radiological_series_get_tag(radiological_series_dict):
    s = RadiologicalSeries.from_dict(radiological_series_dict)
    assert s.get_tag("AcquisitionMatrix") == [0, 377, 377, 0]


@pytest.fixture
def image_metadata_dict():
    return {
        "_id": ObjectId("620cd6773de4068030061460"),
        "dicom_tags": {
            "SpecificCharacterSet": {"vr": "CS", "Value": ["ISO_IR 100"]},
            "ImageType": {
                "vr": "CS",
                "Value": ["DERIVED", "PRIMARY", "W", "W", "DERIVED"],
            },
            "InstanceCreationDate": {"vr": "DA", "Value": ["20200625"]},
            # Many more dicom_tags ...
        },
        "_metadata": {
            "model_version": "0.1.5",
            "modified": datetime.strptime(
                "2022-03-30T12:04:06.727Z", mongo_time_format
            ),
            "created": datetime.strptime("2022-02-16T10:48:23.842Z", mongo_time_format),
        },
        "file_source": {
            "filename": "1.1.11.111111.11.11111.1.1.1111.1111111111111111111.dcm",
            "rel_path": "anonMR_BREAST_2001-3000/5861725858324e5a3132356c43715868693631\
                4331513d3d/Fo3Qxe1aNysvwql1vNG8FSiFSLASHRf5aQjcpxySvgkibwOlW1NkTyr0E4VM\
                2cKPlkHs6/601077_T2W_mDIXON_TSE Tra/1.3.46.670589.11.71565.5.0.9928.201\
                9062510141301624.dcm",
            "drive_name": "TestFolder1",
        },
        "series_id": ObjectId("622b461563ada4de4edbc235"),
    }


def test_image_metadata_from_dict_to_dict(image_metadata_dict):
    s = ImageMetadata.from_dict(image_metadata_dict)
    assert s.to_dict() == image_metadata_dict


def test_image_metadata_from_dataset():
    test_file = get_testdata_file("MR_small.dcm")
    # artificial mount_paths
    mount_paths = {"PymongoData": os.path.dirname(os.path.dirname(test_file))}
    ds = pydicom.dcmread(test_file)
    s = ImageMetadata.from_dataset(ds, mount_paths)
    assert s.file_source.filename == "MR_small.dcm"
    assert s.get_tag("PatientID") == "4MR1"
