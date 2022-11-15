import logging
import os
from copy import copy
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from typing import Callable, List, Tuple, Type, Union

import numpy as np
import pydicom
from bson import ObjectId
from pydicom.dataset import Dataset

from cobra_db import __version__
from cobra_db.dataset_mod import DatasetMod
from cobra_db.enums import Modality, PregnancyStatus
from cobra_db.utils import (
    intersect_dicts,
    parse_AS_as_int,
    parse_DA_as_datetime,
    parse_DA_TM_as_datetime,
)


class BaseObj:
    """Defines the interface to load the object from dict and how to export it to dict."""

    @staticmethod
    def expand(value):
        if isinstance(value, BaseObj):
            return value.to_dict()
        else:
            return value

    def _to_dict(self):
        if is_dataclass(self):
            return self.__dict__
        else:
            raise NotImplementedError

    def to_dict(self):
        """expand the nested objects and remove all keys with None value"""
        d = {k: self.expand(v) for k, v in self._to_dict().items() if v is not None}
        return d

    @classmethod
    def from_dict(cls, d: dict):
        raise NotImplementedError(
            f"This method should be implemented in the child {type(cls)}"
        )


class Embedded(BaseObj):
    """Base class for an embedded doc (that doesn't get its own _id)"""

    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)


@dataclass
class Metadata(Embedded):
    """Stores information of when the db doc was created, modified, and the data model
    version.
    Should be used in any doc with _id that is stored in the database.
    """

    model_version: str
    created: datetime
    modified: datetime = None
    project_name: str = None

    @classmethod
    def create(cls):
        return cls(model_version=__version__, created=datetime.now(timezone.utc))


@dataclass
class Entity(BaseObj):
    """Base class for all entities that are stored in the db with an _id, i.e. that
    require a Dao.
    Usually parent of a ``@dataclass`` class
    """

    _id: ObjectId
    _metadata: Metadata

    def __repr__(self):
        header = f"<{self.__class__.__name__}>"
        body = copy(self.__dict__)
        return f"{header}\n{body}\n"

    def __post_init__(self):
        """Automatically add db_metadata if missing"""
        if self._metadata is None:
            self._metadata = Metadata.create()
        if isinstance(self._metadata, dict):
            self._metadata = Metadata(**self._metadata)

    @classmethod
    def from_dict(cls, obj_dict: dict):
        obj = cls(**obj_dict)
        for f in fields(obj):
            typ = f.type
            if type(typ) != type:
                # TODO: not handled yet!!!
                pass
            else:
                val_dict = obj_dict.get(f.name, None)
                if val_dict is not None and issubclass(typ, BaseObj):
                    obj.__setattr__(f.name, typ.from_dict(val_dict))
        return obj


class Source:
    """Base class for a pointer to more information."""


@dataclass
class DicomEntity(Entity):
    """Base class for all entities that contain dicom_tags."""

    dicom_tags: dict

    @staticmethod
    def optional(ds: Dataset, tag_name: str, parser: Callable = None, default=None):
        """
        Get tag value from dataset, if missing return default value.
        """
        value = ds.get(tag_name, None)
        if value is not None and value != "":
            if parser is not None:
                value = parser(value)
            return value
        else:
            return default

    @staticmethod
    def choose_first(ds, *tag_names, parser, default):
        """
        Choose first tag that does not return None,
        """
        for tag_name in tag_names:
            tag = DicomEntity.optional(ds, tag_name, parser, None)
            if tag is not None:
                return tag
        return default

    @classmethod
    def from_dataset(cls, ds: Dataset):
        """Abstract method. How to create an instance from a pydicom Dataset"""
        raise NotImplementedError(f"Should be implemented by child {type(cls)}")

    def get_tag(self, keyword: str, default=None):
        tag = self.dicom_tags.get(keyword, None)
        if tag is not None:
            value = tag.get("Value", default)
            if not value == default:
                if isinstance(value, list):
                    if len(value) == 1:
                        return value[0]  # most of the cases are this
                return value
        else:
            return default

    def get_acquisition_datetime(self):
        time = self.get_tag("AcquisitionTime")
        time = "000000" if time is None else time
        date = self.get_tag("AcquisitionDate")
        date = "00000000" if date is None else date
        return parse_DA_TM_as_datetime(date, time)


@dataclass
class EntitySource(Embedded, Source):
    """Pointer to another Entity instance in the database.
    Should only be used when there's ambiguity about the referenced class/collection.
    """

    name: str
    description: str
    _entity_type: Union[Type[Entity], str]
    _id: ObjectId

    def __post_init__(self):
        if not type(self._entity_type) is str:
            self._entity_type = self._entity_type.__name__

    def to_dict(self):
        return self.__dict__


@dataclass
class FileSource(Embedded, Source):
    """Pointer to a file in the filesystem.
    rel_path is relative to the place where drive_name is mounted.
    This is because drive_name can be mounted in different paths according
    to the machine and we don't have another way of referencing a drive.
    filename is automatically stored for easy indexing and querying without having to
    manipulate the rel_path to extract the filename.
    """

    drive_name: str
    rel_path: str
    filename: str = None  # automatically set when rel_path is set in __post_init__

    def get_local_filepath(self, mount_paths: dict) -> str:
        """Returns the filepath according to mount_paths"""
        return os.path.join(mount_paths[self.drive_name], self.rel_path)

    def __post_init__(self):
        if self.filename is None:
            self.filename = os.path.basename(self.rel_path)

    @classmethod
    def from_mount_paths(cls, filepath: str, mount_paths: dict):
        """create an instance from the filepath and the mount_paths.

        :param filepath: _description_
        :param mount_paths: _description_
        """
        for drive_name, mount_path in mount_paths.items():
            rel_path = os.path.relpath(filepath, mount_path)
            # TODO: this method will raise ValueError in Windows
            if not rel_path.startswith(".."):  # file in the mount_path
                return cls(drive_name=drive_name, rel_path=rel_path)
        raise ValueError(f"{filepath} is not in any of the mount paths: {mount_paths}")


@dataclass
class Patient(Entity):
    """Represents a unique person that has gone through at least one study.
    https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.2.2.html
    https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.2.3.html
    """

    anon_id: str
    hidden: bool
    birth_date: datetime = None

    def __repr__(self):
        return super().__repr__()

    @classmethod
    def from_dataset(cls, ds: Dataset):
        """
        Create an instance of the entity from a DICOM dataset
        """
        return cls(
            _id=None,
            _metadata=None,
            anon_id=ds.PatientID,
            birth_date=DicomEntity.optional(
                ds, "PatientBirthDate", parse_DA_as_datetime, None
            ),
            hidden=False,
        )

    def __eq__(self, other: "Patient") -> bool:
        return self.anon_id == other.anon_id


@dataclass
class RadiologicalStudy(DicomEntity):
    """Aggregation of multiple dicom instances by the combination of patient and date."""

    study_uid: List[str]  # (0020,0010) LO StudyInstanceUID
    date: datetime  # (0008,0020) DA StudyDate + (0008,0030) TM StudyTime
    series_count: int  # (0020,0011) IS SeriesNumber, Overriden after grouping because
    # the tags are not reliable.
    modality: List[Modality] = None  # (0008,0060) CS Modality
    # SOPClassUID, the meaning can be found in pydicom._uid_dict.UID_dictionaty
    sop_class: List[str] = None
    description: str = None  # (0008,1030) LO StudyDescription
    patient_id: ObjectId = None
    accession_number: List[
        str
    ] = None  # Used to link the study with other administrative registers.
    patient_age: int = None  # in years (0010,1010) AS PatientAge
    patient_weight: float = None  # (0010,1030) PatientWeight
    patient_height: float = None  # (0010,1020) PatientSize
    patient_pregnancy_status: PregnancyStatus = None  # (0010,21C0)

    def __repr__(self):
        return super().__repr__()

    @classmethod
    def from_dataset(cls, ds: Dataset):
        """Create an instance of the entity from a DICOM dataset"""
        try:
            modality = [cls.optional(ds, "Modality", Modality, None)]
        except ValueError as e:
            logging.error(f"{e}")
            modality = None

        return cls(
            _id=None,
            _metadata=None,
            study_uid=cls.optional(ds, "StudyInstanceUID", default=None),
            modality=modality,
            sop_class=[cls.optional(ds, "SOPClassUID", str, None)],
            date=parse_DA_TM_as_datetime(
                cls.optional(ds, "StudyDate", str, "00000000"),
                cls.optional(ds, "StudyTime", str, "000000"),
            ),
            description=cls.optional(ds, "StudyDescription"),
            accession_number=cls.optional(ds, "AccessionNumber", str, None),
            patient_age=cls.optional(ds, "PatientAge", parse_AS_as_int, None),
            patient_weight=cls.optional(ds, "PatientWeight", float, None),
            patient_height=cls.optional(ds, "PatientSize", float, None),
            patient_pregnancy_status=(
                cls.optional(ds, "PatientPregnancyStatus", PregnancyStatus, None)
            ),
            series_count=cls.optional(ds, "SeriesNumber", int, None),
            dicom_tags=DatasetMod.tags_to_keywords(ds.to_json_dict()),
        )

    @classmethod
    def from_dict(cls, d: dict):
        """Convert a mongodb dict into a RadiologicalStudy object"""
        return cls(**d)

    def __eq__(self, other: "RadiologicalStudy") -> bool:
        return self.anon_study_id == other.anon_study_id


@dataclass
class RadiologicalSeries(DicomEntity):
    study_id: ObjectId
    date: datetime = None  # (0008,0021) DA SeriesDate + (0008,0031) TM SeriesTime
    series_uid: str = None  # (0020,000E) UI SeriesInstanceUID
    description: str = None  # 0008103E
    # Overrided by the count in the ImageMetadata collection
    image_count: int = None  # (0020,0013) IS InstanceNumber.
    image_shape: Tuple[int, int] = None  # If the dicom_tags of all images agree.
    protocol_name: str = None  # (0018,1030) LO ProtocolName,
    # other possible tags 00180018, 0008103E, 00181081, 00181250, 00180024, 00181030

    def __repr__(self):
        return super().__repr__()

    @classmethod
    def from_dataset(cls, ds: Dataset):
        """
        Create an instance of the entity from a DICOM dataset
        """
        return cls(
            _id=None,
            _metadata=None,
            series_uid=cls.optional(ds, "SeriesInstanceUID", str, None),
            date=parse_DA_TM_as_datetime(
                cls.optional(ds, "SeriesDate", str, "00000000"),
                cls.optional(ds, "SeriesTime", str, "000000"),
            ),
            study_id=None,
            image_count=cls.optional(ds, "InstanceNumber", int, None),
            protocol_name=cls.optional(ds, "ProtocolName", str, None),
            description=cls.optional(ds, "SeriesDescription", str, None),
            image_shape=(
                cls.optional(ds, "Rows", int, None),
                cls.optional(ds, "Columns", int, None),
            ),
            dicom_tags=DatasetMod.tags_to_keywords(ds.to_json_dict()),
        )

    @classmethod
    def from_images_metadata(cls, images_metadata: List["ImageMetadata"]):
        #  Shared properties are kept
        shared_dicom_tags = intersect_dicts(
            [im_meta.dicom_tags for im_meta in images_metadata]
        )
        _shared_dicom_dict = DatasetMod.keywords_to_tags(
            shared_dicom_tags
        )  # back to dicom hex tags
        shared_ds = Dataset.from_json(_shared_dicom_dict)
        return cls.from_dataset(shared_ds)

    def __eq__(self, other: "RadiologicalSeries") -> bool:
        return self.series_uid == other.series_uid


@dataclass
class ImageMetadata(DicomEntity):
    """Represents one DICOM image i.e. a DICOM instance with PixelData.
    Only the dicom headers are stored. While the pixel data is not stored.
    """

    dicom_tags: dict
    file_source: FileSource
    study_id: ObjectId = None
    series_id: ObjectId = (
        None  # is added by the method ImageMetadataDao.update_series_id()
    )
    aka_file_sources: List[FileSource] = None

    @classmethod
    def from_dataset(cls, ds: Dataset, mount_paths: dict):
        """Create one instance from a pydicom Dataset.

        :param ds: pydicom Dataset
        :param mount_paths: dict that maps the drive_names to the mount path of the
         drives that are in use.
        """
        # sometimes (<1%) the dataset contains mismatches on the VR. For example IS with
        # a float value.
        # in those cases the tag has to be fixed to be able to create the dict.
        json_dict = ds.to_json_dict(suppress_invalid_tags=True)
        dicom_tags = DatasetMod.tags_to_keywords(json_dict)
        return cls(
            _id=None,
            series_id=None,
            _metadata=None,
            file_source=FileSource.from_mount_paths(ds.filename, mount_paths),
            dicom_tags=dicom_tags,
        )

    def to_dataset(self) -> Dataset:
        """Creates a pydicom Dataset with only the metadata in dicom_tags.
        Useful to use pydicom interface when required.
        """
        dicom_tags = DatasetMod.keywords_to_tags(self.dicom_tags)
        return Dataset.from_json(dicom_tags)

    def get_checksum(self, mount_paths: dict) -> str:
        """Obtain the md5 hexdigest hash of the original file. This helps finding
        duplicated images.
        Warning: md5 is not cryptographycally secure.

        :param mount_paths: a dictionary mapping the drive_name to the mount path in the
         local computer.
        :returns: string hexdigest with the hash of the file.
        """
        import hashlib

        filepath = self.get_local_filepath(mount_paths)
        with open(filepath, "rb") as file:
            return hashlib.md5(file.read()).hexdigest()

    def get_local_filepath(self, mount_paths: dict) -> str:
        """Returns the filepath of the file_source according to mount_paths.

        :param mount_paths: a dictionary mapping the drive_name to the mount path in the
         local computer.
        :returns: string hexdigest with the hash of the file.
        """
        return self.file_source.get_local_filepath(mount_paths)

    def get_pixel_data(self, mount_paths) -> np.ndarray:
        """Load the file and extract the pixel_array from the pydicom dataset.

        :param mount_paths: a dictionary mapping the drive_name to the mount path in the
         local computer.
        :returns: the content of pydicom.Dataset.pixel_array
        """
        filepath = self.get_local_filepath(mount_paths)
        ds = pydicom.dcmread(filepath)
        return ds.pixel_array

    def get_file_size(self, mount_paths) -> int:
        """Get the size in bytes of the file"""
        filepath = self.get_local_filepath(mount_paths)
        return os.path.getsize(filepath)

    def __repr__(self):
        return super().__repr__()
