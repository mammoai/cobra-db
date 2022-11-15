import pytest

from cobra_db.model import FileSource, ImageMetadata, Patient
from cobra_db.mongo_dao import Connector, ImageMetadataDao, PatientDao, SeriesDao

# ImageMetadataDao, SeriesDao, StudyDao


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
    assert im.get_local_filepath({"drive_1": "/x"}) == "/x/path_to_file/myfile.dcm"
    im_dao.insert_one(ImageMetadata(None, None, {}, FileSource("1", "2", "222/3.txt")))
    im_dao.update_series_id("62a744bd2f79288ae72033a6", "62a744bd2f79288ae72033a7")


def test_radiological_series_dao(mongodb):
    connector = Connector("test.host.com", 27017, "test_db")
    # the connector db has to be overriden to use the mock database
    connector.db = mongodb
    series_dao = SeriesDao(connector)
    assert series_dao.get_all_ids("62a744bd2f79288ae72033a6") == []
