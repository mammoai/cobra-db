import os
import shutil

import numpy as np
import pydicom
import pytest
from dotenv import load_dotenv

from cobra_db.mongo_dao import Connector
from cobra_db.scripts.pseudonymize_image_metadata import (
    check_mount_paths,
    get_required_drive_names,
    main,
    parse_arguments,
)


@pytest.fixture(scope="module")
def load_env():
    pwd = os.path.dirname(__file__)
    dotenv_path = os.path.join(pwd, ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)


@pytest.fixture(scope="module")
def cfg():
    pwd = os.path.dirname(__file__)

    dotenv_path = os.path.join(pwd, ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    test_config_path = os.path.join(pwd, "script_config/deid.yaml")
    cfg = parse_arguments([test_config_path])
    assert cfg.src_mongo.get("password") == os.environ["MONGOPASS"]
    assert cfg.hash_secret == "A very secret salt for hashing"
    return cfg


def test_get_required_drive_names(cfg):
    assert get_required_drive_names(cfg.src_mongo, {})[0] == ["PseudoPHI"]


def test_missing_drives(cfg):
    with pytest.raises(AssertionError) as exc_info:
        check_mount_paths({}, ["PseudoPHI"], "my_drive")
    assert (
        exc_info.value.args[0] == "Missing configuration for drive_names: {'PseudoPHI'}"
    )


@pytest.mark.slow
def test_main(cfg):
    # Clean up before running
    pwd = os.path.dirname(__file__)
    connector = Connector(**cfg.dst_mongo)
    connector.db.ImageMetadata.drop()
    connector_src = Connector(**cfg.src_mongo)
    dst_path = os.path.join(pwd, "tmp/")
    src_path = os.path.join(pwd, "dicom_data")
    connector_src.db.ImageMetadata.update_many({}, {"$unset": {"aka_file_sources": 1}})
    image_id = connector_src.db.ImageMetadata.find_one(
        {
            "dicom_tags.SOPInstanceUID.Value": "2.25.99060618693674907730262422884187749878"
        },
    )["_id"]

    shutil.rmtree(dst_path, ignore_errors=True)

    # configure paths
    os.environ["DICOM_SRC_PATH"] = src_path
    os.environ["DICOM_DST_PATH"] = dst_path

    # Run the test
    test_config_path = os.path.join(pwd, "script_config/deid.yaml")
    # run for a single image
    main([test_config_path], [image_id])
    assert os.path.exists(
        os.path.join(
            dst_path,
            "rel_path/to/my/dst_path/660/05c/ed21cd\
58352ee70252/study_20002605/series_CT_125800_NEPHRO--4-0--B40f--M-0-4/2.25.9906061869367\
4907730262422884187749878.dcm",
        )
    )
    # run for all images
    main([test_config_path])

    # assert that everything went well
    one_image = connector.db.ImageMetadata.find_one(
        {
            "dicom_tags.PatientID.Value": "66005ced21cd58352ee70252b884319ea72b6c9a51715\
87dcf68c9340191dc7b"
        }
    )
    assert one_image is not None
    src_ds = pydicom.read_file(
        os.path.join(
            src_path,
            "6670427471/05-26-2000-NA-FORFILE CT ABD ANDOR PEL - CD-25398/5.000000-NEPHR\
O  4.0  B40f  M0.4-18678/1-007.dcm",
        )
    )
    dst_ds = pydicom.read_file(
        os.path.join(
            dst_path,
            "rel_path/to/my/dst_path/660/05c/ed21cd58352ee70252/study_20002605/series_CT\
_125800_NEPHRO--4-0--B40f--M-0-4/2.25.29709809808460026253134446579710279158.dcm",
        )
    )

    assert np.all(src_ds.pixel_array == dst_ds.pixel_array)


@pytest.mark.slow
def test_main_multiproc(cfg):
    # Clean up before running
    pwd = os.path.dirname(__file__)
    connector = Connector(**cfg.dst_mongo)
    connector.db.ImageMetadata.drop()
    connector_src = Connector(**cfg.src_mongo)
    dst_path = os.path.join(pwd, "tmp/")
    src_path = os.path.join(pwd, "dicom_data")
    connector_src.db.ImageMetadata.update_many({}, {"$unset": {"aka_file_sources": 1}})
    image_id = connector_src.db.ImageMetadata.find_one(
        {
            "dicom_tags.SOPInstanceUID.Value": "2.25.99060618693674907730262422884187749878"
        },
    )["_id"]

    shutil.rmtree(dst_path, ignore_errors=True)

    # configure paths
    os.environ["DICOM_SRC_PATH"] = src_path
    os.environ["DICOM_DST_PATH"] = dst_path

    # Run the test
    test_config_path = os.path.join(pwd, "script_config/deid_multiproc.yaml")
    # run for a single image
    main([test_config_path], [image_id])
    assert os.path.exists(
        os.path.join(
            dst_path,
            "rel_path/to/my/dst_path/660/05c/ed21cd\
58352ee70252/study_20002605/series_CT_125800_NEPHRO--4-0--B40f--M-0-4/2.25.9906061869367\
4907730262422884187749878.dcm",
        )
    )
    # run for all images
    main([test_config_path])

    # assert that everything went well
    one_image = connector.db.ImageMetadata.find_one(
        {
            "dicom_tags.PatientID.Value": "66005ced21cd58352ee70252b884319ea72b6c9a51715\
87dcf68c9340191dc7b"
        }
    )
    assert one_image is not None
    src_ds = pydicom.read_file(
        os.path.join(
            src_path,
            "6670427471/05-26-2000-NA-FORFILE CT ABD ANDOR PEL - CD-25398/5.000000-NEPHR\
O  4.0  B40f  M0.4-18678/1-007.dcm",
        )
    )
    dst_ds = pydicom.read_file(
        os.path.join(
            dst_path,
            "rel_path/to/my/dst_path/660/05c/ed21cd58352ee70252/study_20002605/series_CT\
_125800_NEPHRO--4-0--B40f--M-0-4/2.25.29709809808460026253134446579710279158.dcm",
        )
    )

    assert np.all(src_ds.pixel_array == dst_ds.pixel_array)
