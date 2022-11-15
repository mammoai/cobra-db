import logging
import os
from datetime import datetime, timezone
from email.generator import Generator
from getpass import getpass
from typing import List, Tuple, Type, Union
from urllib.parse import quote

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from cobra_db.model import (
    Entity,
    FileSource,
    ImageMetadata,
    Patient,
    RadiologicalSeries,
    RadiologicalStudy,
)
from cobra_db.types import Id


def get_obj_id(_id: Id) -> ObjectId:
    """
    Get ObjectId regardless of the initial type
    """
    return _id if isinstance(_id, ObjectId) else ObjectId(_id)


class Connector:
    """
    Connect and auth to a mongo database.
    """

    def __init__(
        self,
        host: str,
        port: int,
        db_name: str,
        username: str = None,
        password: str = None,
    ):
        """Create a new instance of the Connector.
        Remember that passwords should not be stored in plain text and
        you can use the `get_env_pass` and `get_pass` methods as alternative
        methods for inputting the password.

        :param host: url of the mongodb host. Examples "192.168.1.10",
         "localhost"
        :param port: int specifying the port of your database. The most common
         is 27017
        :param db_name: name of the database you want to access
        :param username: your username, defaults to None
        :param password: your password, defaults to None
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.connect()

    def connect(self):
        """
        Connect to the mongo database
        """
        self.client = MongoClient(
            self._get_uri(self.host, self.port, self.username, self.password),
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
        )

        self.db = self.client[self.db_name]

    @staticmethod
    def _get_uri(host: str, port: str, username=None, password=None, db_name=""):
        if host.endswith("mongodb.net"):
            q_username = quote(username)
            q_password = quote(password)
            return f"mongodb+srv://{q_username}:{q_password}@{host}/{db_name}"
        if username is None:
            return f"mongodb://{host}:{port}/{db_name}"
        elif password is None:
            return f"mongodb://{quote(username)}@{host}:{port}/{db_name}"
        else:
            q_username = quote(username)
            q_password = quote(password)
            return f"mongodb://{q_username}:{q_password}@{host}:{port}/{db_name}"

    def close(self):
        """
        Close the connection to the database
        """
        self.client.close()

    @classmethod
    def get_pass(cls, host: str, port: int, db_name: str, username: str):
        """Create a Connector instance by prompting the user for a password.
        Most useful in jupyter notebooks.
        """
        password = getpass(
            f"Password for {cls._get_uri(host, port, username, db_name)} "
        )
        assert password != "" and password is not None, "Password is empty"
        return cls(host, port, db_name, username, password)

    @classmethod
    def get_env_pass(
        cls,
        host: str,
        port: int,
        db_name: str,
        username: str,
        env_var: str = "MONGOPASS",
    ):
        """Create a Connector instance by getting the password from the environment
        variable defined in env_var. Useful when creating scripts.
        """
        password = os.environ.get(env_var, None)
        assert (
            password is not None
        ), f"Env variable {env_var} is empty, please run \n\
             export {env_var}='mypassword'\n\
            and then try again."
        return cls(host, port, db_name, username, password)

    def __str__(self):
        if self.password is not None:
            safe_password = "".join(["*" for _ in self.password])
        else:
            safe_password = None
        return self._get_uri(
            self.host, self.port, self.username, safe_password, self.db_name
        )


class EntityDao:
    """
    Base class for all entity DAOs.
    Children need to implement a concrete to_dict() method to be able to use
    get_by_id(x, obj=True)
    """

    def __init__(self, connector: Connector, entity_type: Type[Entity]):
        self.connector = connector
        self.entity_type = entity_type
        self.collection: Collection = self._get_collection(self.entity_type)

    def _get_collection(self, entity_type: Type[Entity]) -> Collection:
        """
        Get a collection from the database according to the entity type
        """
        return self.connector.db[entity_type.__name__]

    def get_by_id(self, _id: Id, obj=True) -> Union[Entity, dict]:
        """Find a document by _id."""
        _id = get_obj_id(_id)
        answers = [i for i in self.collection.find({"_id": _id})]
        assert (
            len(answers) == 1
        ), f"{self.__class__.__name__}: The number of docs found for id {_id} is \
            {len(answers)}"
        obj_dict = answers[0]
        if obj:
            if (
                getattr(self.entity_type, "from_dict", None) is None
            ):  # check that the method was implemented
                raise NotImplementedError(
                    f"{self.entity_type} needs to implement a 'from_dict' method"
                )
            return self.entity_type.from_dict(obj_dict)
        else:
            return obj_dict

    def find(self, filter: dict = {}, **kwargs) -> Generator:
        """
        Generate instances for the documents that match the filter.
        This is not a very fast approach, but it is easy to use.

        filter: dict, valid mongodb query to find the desired entities.
        kwargs to pass to pymongo.Collection.find(filter, **kwargs)
        """
        for entity in self.collection.find(filter, **kwargs):
            yield self.entity_type.from_dict(entity)

    def delete_by_id(self, _id: Id):
        """Delete a document from the database by its id.

        :param _id: ObjectId or str version of ObjectId of the document that will be
        deleted.
        """
        _id = get_obj_id(_id)
        result = self.collection.delete_one({"_id": _id})
        assert result.deleted_count == 1


class PatientDao(EntityDao):
    def __init__(self, connector):
        super().__init__(connector, Patient)
        self._index_exists = False

    def _ensure_index_exists(self):
        """
        Check if the index for anon_id exists
        """
        if not self._index_exists:
            self.collection.create_index("anon_id", unique=True)
            self._index_exists = True

    def anon_id_to_mongo_id(self, anon_id: str) -> ObjectId:
        """
        Get the _id of a patient from its anon_id
        """
        patient = self.collection.find_one({"anon_id": anon_id})
        if patient is None:
            raise IndexError(f"Patient {anon_id} does not exist")
        return patient["_id"]

    def get_patient_by_id(self, _id: Id = None, obj=True) -> Union[Patient, dict]:
        """
        Get a patient from its _id
        """
        _id = get_obj_id(_id)
        patient = self.collection.find_one({"_id": _id})
        if patient is None:
            raise IndexError(f"Patient with _id: {_id} does not exist")
        if obj:
            return Patient(**patient)
        else:
            return patient

    def get_patient_by_anon_id(self, anon_id: str, obj=True) -> Union[Patient, dict]:
        """
        Get a patient from its anon_id
        """
        patient = self.collection.find_one({"anon_id": anon_id})
        if patient is None:
            raise IndexError(f"Patient with anon_id: {anon_id} does not exist")
        if obj:
            return Patient(**patient)
        else:
            return patient

    def get_patient(self, patient: Patient, obj=True) -> Union[Patient, dict]:
        """
        Get a patient if it exists in the database.
        Expect an IndexError if it does not exist.
        """
        if patient._id is not None:
            get_patient = self.get_patient_by_id
            identifier = patient._id
        elif patient.anon_id is not None:
            get_patient = self.get_patient_by_anon_id
            identifier = patient.anon_id
        else:
            raise IndexError(f"Patient {patient} does not have a _id or anon_id")
        patient = get_patient(identifier, obj=False)
        if obj:
            return Patient(**patient)
        else:
            return patient

    def exists(self, patient: Patient) -> Tuple[bool, Union[ObjectId, None]]:
        try:
            return True, self.get_patient(patient, obj=False)["_id"]
        except IndexError:
            return False, None

    def insert_one(self, patient: Patient, skip_duplicates=True):
        """
        Insert a patient into the database.
        raises DuplicateKeyError if the patient already exists and check_unique is False
        """
        if skip_duplicates:
            exists, _id = self.exists(patient)
            if exists:
                logging.info(
                    f"Skipping insertion. Patient already exists in the database with \
                    _id {_id}"
                )
                return _id
        _id = self.collection.insert_one(patient.to_dict()).inserted_id
        self._ensure_index_exists()
        return _id

    def get_by_id(self, _id: Id, obj=True) -> Union[Patient, dict]:
        """redifining only for type hinting"""
        return super().get_by_id(_id, obj)


class StudyDao(EntityDao):
    def __init__(self, connector):
        super().__init__(connector, RadiologicalStudy)
        self.patient_dao = PatientDao(connector)
        self._index_exists = False

    def _ensure_index_exists(self):
        """
        Check if the index for study_uid exists
        """
        if not self._index_exists:
            self.collection.create_index(
                [("dicom_tags.PatientID.Value", 1), ("date", 1)], unique=True
            )
            self._index_exists = True

    def insert_one(self, study: RadiologicalStudy, skip_duplicates=True):
        """
        Insert a study into the database.
        raises DuplicateKeyError if the study already exists and skip_duplicates is
        False
        """
        assert isinstance(
            study, RadiologicalStudy
        ), f"type of study should be RadiologicalStudy, not {type(study)}"
        if skip_duplicates:
            try:
                _id = self.collection.insert_one(study.to_dict()).inserted_id
            except DuplicateKeyError:
                logging.info(
                    f"Skipping insertion. Study already exists in the database with \
                    study_uid {study.study_uid}"
                )
                _id = self.collection.find_one(
                    {
                        "dicom_tags.PatientID.Value": study.get_tag("PatientID"),
                        "date": study.date,
                    }
                )["_id"]
        else:
            _id = self.collection.insert_one(study.to_dict()).inserted_id
        self._ensure_index_exists()
        return _id

    def update_patient_id(self, study_id: Id, patient_id: Id):
        """Once the patient doc is created the series_id of the metadata should be
        updated
        """
        return self.collection.update_one(
            {"_id": get_obj_id(study_id)},
            {
                "$set": {
                    "patient_id": get_obj_id(patient_id),
                    "_metadata.modified": datetime.now(timezone.utc),
                }
            },
        )

    def get_all_ids(
        self, patient: Union[Patient, Id], *modality: str, other_filters: dict = {}
    ) -> List[ObjectId]:
        """
        Given a patient or patient_id, retrieve all studies that match any of
        the specified modality. other_filters allows you to narrow down the query.
        """
        patient_id = (
            get_obj_id(patient._id)
            if isinstance(patient, Patient)
            else get_obj_id(patient)
        )
        query = {
            "patient_id": patient_id,
            "modality": {"$in": modality},
            **other_filters,
        }
        ids = [i["_id"] for i in self.collection.find(query, {"_id": 1})]
        return ids

    def get_by_id(self, _id: Id, obj=True) -> Union[RadiologicalStudy, dict]:
        """Get a RadiologicalStudy by _id"""
        # redifining only for type hinting
        return super().get_by_id(_id, obj)


class SeriesDao(EntityDao):
    def __init__(self, connector):
        super().__init__(connector, RadiologicalSeries)
        self.study_dao = StudyDao(connector)
        self._index_exists = False

    def _ensure_index_exists(self):
        """
        Check if the index for anon_series_id exists
        """
        if not self._index_exists:
            self.collection.create_index(
                [("series_uid", 1), ("study_id", 1)], unique=True
            )
            self._index_exists = True

    def insert_one(
        self,
        series: RadiologicalSeries,
        study: RadiologicalStudy = None,
        skip_duplicates=True,
    ):
        """
        Insert a series into the database first retrieving the study id.
        raises DuplicateKeyError if the series already exists and skip_duplicates is
        False
        """
        if series.study_id is None and study is not None:
            series = self.obtain_study_id(series, study)
        if skip_duplicates:
            try:
                _id = self.collection.insert_one(series.to_dict()).inserted_id
            except DuplicateKeyError:
                logging.info(
                    f"Skipping insertion. Series already exists in the database with \
                        series_uid {series.series_uid}"
                )
                _id = self.collection.find_one({"series_uid": series.series_uid})["_id"]
        else:
            _id = self.collection.insert_one(series.to_dict()).inserted_id
        self._ensure_index_exists()
        return _id

    def update_study_id(self, series_id: Id, study_id: Id):
        """Once the series doc is created the series_id of the metadata should be
        updated
        """
        return self.collection.update_one(
            {"_id": get_obj_id(series_id)},
            {
                "$set": {
                    "study_id": get_obj_id(study_id),
                    "_metadata.modified": datetime.now(timezone.utc),
                }
            },
        )

    def get_all_ids(
        self, study: Union[RadiologicalStudy, Id], other_filters: dict = {}
    ):
        """
        Given a study or study_id, retrieve all series that belong to this study.
        other_filter can be used to narrow down the query.
        """
        study_id = (
            get_obj_id(study._id)
            if isinstance(study, RadiologicalStudy)
            else get_obj_id(study)
        )
        query = {
            "study_id": study_id,
            **other_filters,
        }
        return [i["_id"] for i in self.collection.find(query, {"_id": 1})]

    def get_by_id(self, _id: Id, obj=True) -> Union[RadiologicalSeries, dict]:
        """Get RadiologicalSeries by _id"""
        # redifining only for type hinting
        return super().get_by_id(_id, obj)


class ImageMetadataDao(EntityDao):
    def __init__(self, connector):
        super().__init__(connector, ImageMetadata)
        self.series_dao = SeriesDao(connector)
        self._index_exists = False

    def _ensure_index_exists(self):
        """
        Check if the index for anon_image_id exists
        """
        if not self._index_exists:
            self.collection.create_index(
                [("image_uid", 1), ("series_id", 1)], unique=True
            )
            self._index_exists = True

    def insert_one(self, im_metadata: ImageMetadata):
        _id = self.collection.insert_one(im_metadata.to_dict()).inserted_id
        return _id

    def update_series_id(self, im_meta_id: Id, series_id: Id):
        """Once the series doc is created the series_id of the metadata should be
        updated
        """
        return self.collection.update_one(
            {"_id": get_obj_id(im_meta_id)},
            {
                "$set": {
                    "series_id": get_obj_id(series_id),
                    "_metadata.modified": datetime.now(timezone.utc),
                }
            },
        )

    def update_study_id(self, im_meta_id: Id, study_id: Id):
        """Once the series doc is created the series_id of the metadata should be
        updated
        """
        return self.collection.update_one(
            {"_id": get_obj_id(im_meta_id)},
            {
                "$set": {
                    "study_id": get_obj_id(study_id),
                    "_metadata.modified": datetime.now(timezone.utc),
                }
            },
        )

    def get_all_ids(
        self, series: Union[RadiologicalSeries, Id], other_filters: dict = {}
    ):
        """Given a series or series_id, retrieve all images that belong to this series.
        other_filter can be used to narrow down the query.
        """
        study_id = (
            get_obj_id(series._id)
            if isinstance(series, RadiologicalSeries)
            else get_obj_id(series)
        )
        query = {
            "series_id": study_id,
            **other_filters,
        }
        return [i["_id"] for i in self.collection.find(query, {"_id": 1})]

    def get_by_id(self, _id: Id, obj=True) -> Union[ImageMetadata, dict]:
        """Get an ImageMetadata by _id"""
        # redifining only for type hinting
        return super().get_by_id(_id, obj)

    def add_aka(self, im_meta_id: Id, file_source: FileSource):
        """Adds an AKA file source. This is useful when a pseudonymized version of the
        file is saved somewhere else.
        :param im_meta_id: The id of the document that will be updated
        :param file_source: The AKA filesource
        """
        return self.collection.update_one(
            {"_id": get_obj_id(im_meta_id)},
            {
                "$set": {
                    "_metadata.modified": datetime.now(timezone.utc),
                },
                "$push": {
                    "aka_file_sources": file_source.to_dict(),
                },
            },
        )
