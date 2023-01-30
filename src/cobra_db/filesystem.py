import os
import re
from datetime import datetime

from pydicom import Dataset

from cobra_db.model import DicomEntity
from cobra_db.utils import parse_DA_TM_as_datetime

non_alphanumeric = re.compile(r"[^a-zA-Z\d]")
opt = DicomEntity.optional


def get_patient_path(patient_anon_id: str):
    """Obtains a relative folder with the first 12 Bytes (24 chars of the hexstring):
    The hashed PatientID is
    26e3af1f7f22df92d6185a8e15ebdfc0ad089a17ec516377525f42c016f1d5cb
    the folder is
    26e/3af/1f7f22df92d6185a8e/
    This will balance the folder structure given that the hash is almost random."""
    if patient_anon_id is None:
        return "UNK_PatientID"
    assert len(patient_anon_id) == 64, "Length of patient hash is incorrect"
    assert int(patient_anon_id, 16), "Non hex values in string"
    p = patient_anon_id
    return os.path.join(p[0:3], p[3:6], p[6:24])


def get_study_path(patient_anon_id: str, study_date: datetime):
    """Our definition of study is PatientID + StudyDate. This method helps keeping the
    folder structure in the same way"""
    if study_date is None:
        study_date = datetime(1900, 1, 1)
    return os.path.join(
        get_patient_path(patient_anon_id), f'study_{study_date.strftime("%Y%d%m")}'
    )


def get_series_path(ds: Dataset):
    """Get a unique path for a dataset"""

    study_dt = parse_DA_TM_as_datetime(
        opt(ds, "StudyDate", str, "00000000"),
        opt(ds, "StudyTime", str, "000000"),
    )
    series_dt = parse_DA_TM_as_datetime(
        opt(ds, "SeriesDate", str, "00000000"),
        opt(ds, "SeriesTime", str, "000000"),
    )
    if series_dt is None:
        series_key = opt(ds, "SeriesNumber", str, "UNK")
    else:
        series_key = series_dt.strftime("%H%M%S")
    modality = opt(ds, "Modality", str, "UNK")
    description = opt(ds, "SeriesDescription", str, "UNK")
    description = non_alphanumeric.sub("-", description)
    patient_id = ds.get("PatientID", None)
    return os.path.join(
        get_study_path(patient_id, study_dt),
        f"series_{modality}_{series_key}_{description}",
    )


def get_instance_path(ds: Dataset):
    """Get unique path for instance"""
    key = ds.SOPInstanceUID
    return os.path.join(get_series_path(ds), f"{key}.dcm")
