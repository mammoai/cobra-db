from dataclasses import dataclass
from pathlib import Path

import pytest

from cobra_db.model import (
    Entity, 
    FileSource, 
    ImageMetadata, 
    Patient)
from cobra_db.mongo_dao import (
    Connector,
    EntityDao,
    ImageMetadataDao,
    PatientDao,
    SeriesDao,
)

# ImageMetadataDao, SeriesDao, StudyDao, EthicalApprovalDao


@pytest.mark.parametrize(
    "host,port,db_name,username,password",
    (
        ("test_host", 27017, "test_db", None, None),
        ("test_host", 27017, "test_db", "test_user", "test_password"),
    ),
)
def test_connector(host, port, db_name, username, password):
    connector = Connector(
        host=host,
        port=port,
        db_name=db_name,
        username=username,
        password=password,
    )
    assert isinstance(connector, Connector)


def test_patient_dao(mongodb):
    connector = Connector("test.host.com", 27017, "test_db")
    # the connector db has to be overriden to use the mock database
    connector.db = mongodb
    patient_dao = PatientDao(connector)
    x = list(patient_dao.find())[0]
    assert x.anon_id == "12345678900"
    patient_dao.get_by_id(x._id)
    new_id = patient_dao.insert_one(
        Patient(None, None, "2345667", True, birth_date=None)
    )
    patient_dao.get_by_id(new_id)


def test_image_metadata_dao(mongodb):
    connector = Connector("test.host.com", 27017, "test_db")
    # the connector db has to be overriden to use the mock database
    connector.db = mongodb
    im_dao = ImageMetadataDao(connector)
    im = im_dao.get_by_id("62a744bd2f79288ae72033a6")
    assert im.get_acquisition_datetime() is None
    assert Path(im.get_local_filepath({"drive_1": "/x"})) == Path(
        "/x/path_to_file/myfile.dcm"
    )
    im_dao.insert_one(ImageMetadata(None, None, {}, FileSource("1", "2", "222/3.txt")))
    im_dao.update_series_id("62a744bd2f79288ae72033a6", "62a744bd2f79288ae72033a7")


def test_radiological_series_dao(mongodb):
    connector = Connector("test.host.com", 27017, "test_db")
    # the connector db has to be overriden to use the mock database
    connector.db = mongodb
    series_dao = SeriesDao(connector)
    assert series_dao.get_all_ids("62a744bd2f79288ae72033a6") == []


def test_entity_dao_insert_one(mongodb):
    connector = Connector("test.host.com", 27017, "test_db")
    # the connector db has to be overriden to use the mock database
    connector.db = mongodb

    # Define a new simple class
    @dataclass
    class FunnyEntity(Entity):
        joke: str
        joke_response: str = None

    # Create a Dao
    entity_dao = EntityDao(connector, FunnyEntity)

    funny_instance = FunnyEntity(_id=None, _metadata=None, joke="Whos this?")
    _id = entity_dao.insert_one(funny_instance)
    instance_dict = connector.db.FunnyEntity.find_one({"_id": _id})
    assert instance_dict["joke"] == "Whos this?"
    assert instance_dict.get("joke_response", 1) == 1
