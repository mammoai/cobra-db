import os

import pytest
from dotenv import load_dotenv

from cobra_db.mongo_dao import Connector
from cobra_db.scripts.stage_1_ingest_images import main, parse_arguments


def test_parse_args():
    raw_args = "\
        --drive_names MammoAiHD1 MammoAiHD2 \
        --drive_paths /media/hd1 /media/hd2 \
        --host localhost \
        --port 27017  \
        --username my_user \
        --database_name my_db \
        --num_processes 1\
        --project_name vaib"
    raw_args = [x for x in raw_args.split(" ") if len(x) > 0]
    mount_paths, connector_kwargs, n_proc, project_name = parse_arguments(raw_args)
    assert mount_paths == {"MammoAiHD1": "/media/hd1", "MammoAiHD2": "/media/hd2"}
    assert connector_kwargs == {
        "host": "localhost",
        "port": 27017,
        "username": "my_user",
        "db_name": "my_db",
    }
    assert n_proc == 1
    assert project_name == "vaib"


@pytest.mark.slow
def test_single_proc():
    """We use the following resource to be able to run this test
    https://wiki.cancerimagingarchive.net/download/attachments/80969777/Pseudo-Phi-DICOM%20Evaluation%20dataset%20April%207%202021.tcia?api=v2
    For download instructions, please refer to https://www.youtube.com/watch?v=NO48XtdHTic
    """
    pwd = os.path.dirname(__file__)
    dotenv_path = os.path.join(pwd, ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    # Start the test
    data_path = os.path.join(pwd, "dicom_data")
    raw_args = f"\
        --drive_names PseudoPHI \
        --drive_paths {data_path} \
        --host {os.environ['MONGOHOST']} \
        --username {os.environ['MONGOUSER']}\
        --database_name test_db\
        --num_processes 1\
        --project_name test_project"
    raw_args = [x for x in raw_args.split(" ") if len(x) > 0]
    mount_paths, connector_kwargs, n_proc, project_name = parse_arguments(raw_args)

    # Clean up before running
    connector = Connector.get_env_pass(**connector_kwargs)
    connector.db.ImageMetadata.drop()

    main(mount_paths, connector_kwargs, n_proc, project_name)


@pytest.mark.slow
def test_multi_proc():
    """We use the following resource to be able to run this test
    https://wiki.cancerimagingarchive.net/download/attachments/80969777/Pseudo-Phi-DICOM%20Evaluation%20dataset%20April%207%202021.tcia?api=v2
    For download instructions, please refer to https://www.youtube.com/watch?v=NO48XtdHTic
    """
    pwd = os.path.dirname(__file__)
    dotenv_path = os.path.join(pwd, ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    data_path = os.path.join(pwd, "dicom_data")
    raw_args = f"\
        --drive_names PseudoPHI \
        --drive_paths {data_path} \
        --host {os.environ['MONGOHOST']} \
        --username {os.environ['MONGOUSER']}\
        --database_name test_db\
        --num_processes 2\
        --project_name test_project"
    raw_args = [x for x in raw_args.split(" ") if len(x) > 0]
    mount_paths, connector_kwargs, n_proc, project_name = parse_arguments(raw_args)

    # Clean up before running
    connector = Connector.get_env_pass(**connector_kwargs)
    connector.db.ImageMetadata.drop()

    main(mount_paths, connector_kwargs, n_proc, project_name)
