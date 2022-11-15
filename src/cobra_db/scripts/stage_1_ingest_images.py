"""
usage: cobra_ingest [-h]
 -dn DRIVE_NAMES [DRIVE_NAMES ...]
 -dp DRIVE_PATHS [DRIVE_PATHS ...]
 [-ho HOST] [-p PORT] [-u USERNAME] -db DATABASE_NAME [-n NUM_PROCESSES]
 [--project_name PROJECT_NAME]

Recursively ingest a set of directories of dicom files into a mongo database.

optional arguments:
  -h, --help            show this help message and exit
  -dn DRIVE_NAMES [DRIVE_NAMES ...], --drive_names DRIVE_NAMES [DRIVE_NAMES ...]
                        the name of the drives to ingest
  -dp DRIVE_PATHS [DRIVE_PATHS ...], --drive_paths DRIVE_PATHS [DRIVE_PATHS ...]
                        the path where the drives are mounted (in the same order as
                        drive_names)
  -ho HOST, --host HOST
                        MongoDB host
  -p PORT, --port PORT  MongoDB port
  -u USERNAME, --username USERNAME
                        MongoDB username
  -db DATABASE_NAME, --database_name DATABASE_NAME
                        MongoDB database
  -n NUM_PROCESSES, --num_processes NUM_PROCESSES
                        Number of processes to use. It runs single threaded by default
                        so that it can be easily debugged
  --project_name PROJECT_NAME
                        A project name that will be stored in the _metadata field of the
                         created ImageMetadata docs
"""

import logging
import os
import sys
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool
from typing import List, Tuple

import pydicom
from tqdm import tqdm

from cobra_db.model import ImageMetadata
from cobra_db.mongo_dao import Connector, ImageMetadataDao
from cobra_db.scripts.utils import add_args_to_iterable, batcher
from cobra_db.utils import find_dcm


def parse_arguments(raw_args: List[str]) -> Tuple[str, dict, int, str]:
    """Convert the console arguments into python variables.

    :param raw_args: the list of commands (usually sys.argv[1:])
    :return: mount_paths, connector_kwargs, n_proc
    """
    parser = ArgumentParser(
        description="Recursively ingest a set of directories \
            of dicom files into a mongo database."
    )
    parser.add_argument(
        "-dn",
        "--drive_names",
        nargs="+",
        help="the name of the drives to ingest",
        required=True,
    )
    parser.add_argument(
        "-dp",
        "--drive_paths",
        nargs="+",
        help="the path where the drives are mounted (in the same order as drive_names)",
        required=True,
    )
    parser.add_argument("-ho", "--host", help="MongoDB host", default="127.0.0.1")
    parser.add_argument("-p", "--port", help="MongoDB port", default=27027, type=int)
    parser.add_argument("-u", "--username", help="MongoDB username", default=None)
    parser.add_argument(
        "-db", "--database_name", help="MongoDB database", required=True
    )
    parser.add_argument(
        "-n",
        "--num_processes",
        help="Number of processes to use. \
        It runs single threaded by default so that it can be easily debugged",
        default=1,
        type=int,
    ),
    parser.add_argument(
        "--project_name",
        help="A project name that will be stored in the _metadata field \
            of the created ImageMetadata docs",
    )

    args = parser.parse_args(raw_args)
    assert len(args.drive_paths) == len(args.drive_names)
    mount_paths = {n: d for n, d in zip(args.drive_names, args.drive_paths)}
    connector_kwargs = {
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "db_name": args.database_name,
    }
    return mount_paths, connector_kwargs, args.num_processes, args.project_name


def ingest_metadata(
    filepath: str, im_dao: ImageMetadataDao, mount_paths: dict, project_name
) -> int:
    """Ingest a single dicom file's metadata into the database.
    return: 1 if it was ingested properly.
    """
    try:
        ds = pydicom.dcmread(filepath, stop_before_pixels=True)
        # private tags are causing problems because they are
        # not standard. We are not indexing them.
        ds.remove_private_tags()
        image_metadata = ImageMetadata.from_dataset(ds, mount_paths)
        image_metadata._metadata.project_name = project_name
        im_dao.insert_one(image_metadata)
        return 1
    # except OperationFailure as e:
    #     raise e
    # except FileNotFoundError as e:
    #     raise e
    except Exception as e:
        logging.error(f"{e} - filepath:{filepath}")
    return 0


def process_files_batch(args) -> Tuple[int, int]:
    """Process a batch of files.

    :param args: Single argument tuple of (filepaths, im_dao, mount_paths).
     It gives the ability to use multiprocessing.
    :return: number of seen files and number of correctly processed files
    """
    filepaths, connector_kwargs, mount_paths, project_name = args
    connector = Connector.get_env_pass(**connector_kwargs)
    im_dao = ImageMetadataDao(connector)
    seen = len(filepaths)
    ingested = 0
    for filepath in filepaths:
        ingested += ingest_metadata(filepath, im_dao, mount_paths, project_name)
    return seen, ingested


def multiproc_drive(args):
    mount_paths, connector_kwargs, n_proc, drive_name, drive_path, project, pos = args
    logging.warning(f"Ingesting {drive_name} on path {drive_path}")
    print(f"Ingesting {drive_name}")
    pbar_seen = tqdm(
        smoothing=0, desc=f"{drive_name} Seen", unit="dcm", position=pos * 2
    )
    pbar_ingested = tqdm(
        smoothing=0, desc=f"{drive_name} Ingested", unit="dcm", position=pos * 2 + 1
    )
    filepaths = find_dcm(drive_path)
    args_gen = add_args_to_iterable(
        batcher(filepaths, 1000), connector_kwargs, mount_paths, project
    )
    pool = Pool(n_proc)
    total_ingested = 0
    for seen, ingested in pool.imap_unordered(process_files_batch, args_gen):
        pbar_seen.update(seen)
        pbar_ingested.update(ingested)
        total_ingested += ingested
    return total_ingested


def multiproc(
    mount_paths: dict, connector_kwargs: dict, n_proc: int, project_name: str
):
    # ingest each drive
    drive_tasks = []
    for i, (drive_name, drive_path) in enumerate(mount_paths.items()):
        drive_tasks.append(
            [
                mount_paths,
                connector_kwargs,
                n_proc,
                drive_name,
                drive_path,
                project_name,
                i,
            ]
        )
    total_ingested = 0

    with ProcessPoolExecutor(len(drive_tasks)) as executor:
        for ingested in executor.map(multiproc_drive, drive_tasks):
            total_ingested += ingested

    print(f"Finished ingesting {len(mount_paths)} drive(s)")
    return total_ingested


def single_proc(mount_paths: dict, connector_kwargs: dict, project_name: str):
    """Run the same thing as multiproc but single threaded

    :param mount_paths: Where the drives are
    :param connector_kwargs: How to connect to the db
    :param project_name: name of the project that the images belong to
    """
    for drive_name, drive_path in mount_paths.items():
        logging.info(f"Ingesting {drive_name}")
        print(f"Ingesting {drive_name}")
        pbar_seen = tqdm(smoothing=0, desc="Seen", unit="dcm")
        pbar_ingested = tqdm(smoothing=0, desc="Ingested", unit="dcm")
        filepaths = find_dcm(drive_path)
        args_gen = add_args_to_iterable(
            batcher(filepaths, 10), connector_kwargs, mount_paths, project_name
        )
        for args in args_gen:
            seen, ingested = process_files_batch(args)
            pbar_seen.update(seen)
            pbar_ingested.update(ingested)
    print(f"Finished ingesting {len(mount_paths)} drives")


def main(mount_paths, connector_kwargs, n_proc, project_name):
    # configure logging
    # now = datetime.now().strftime("%Y%m%d_%H%M")
    # log_path = f"stage1_{now}.log"
    logging.basicConfig(
        # filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.warning(
        f"Starting Stage1 with {len(mount_paths)} drives and {n_proc} processes per drive"
    )

    # quick check to make sure we have the drives connected
    for drive_name, mount_path in mount_paths.items():
        assert os.path.exists(
            mount_path
        ), f"Drive {drive_name} does not exist in {mount_path}"

    # ingest everything
    if n_proc == 1:
        total_ingested = single_proc(mount_paths, connector_kwargs, project_name)
    else:
        total_ingested = multiproc(mount_paths, connector_kwargs, n_proc, project_name)

    # Health check afterwards
    im_dao = ImageMetadataDao(Connector.get_env_pass(**connector_kwargs))
    total_images = im_dao.collection.count_documents({})
    print(total_images)

    logging.warning(
        f"Total ingested = {total_ingested},\n\
        total images in {str(im_dao.connector)} = {total_images}"
    )
    logging.warning(
        "Creating collection indices for SOPInstanceUID, \
        SeriesInstanceUID, StudyInstanceUID. \
        See https://www.mongodb.com/docs/manual/indexes/"
    )
    for i in ["SOPInstanceUID", "SeriesInstanceUID", "StudyInstanceUID"]:
        try:
            im_dao.collection.create_index(f"dicom_tags.{i}.Value")
        except Exception as e:
            print(e)
    with_SOPInstanceUID = im_dao.collection.count_documents(
        {"dicom_tags.SOPInstanceUID.Value": {"$exists": True}}
    )
    with_SeriesInstanceUID = im_dao.collection.count_documents(
        {"dicom_tags.SOPInstanceUID.Value": {"$exists": True}}
    )
    with_StudyInstanceUID = im_dao.collection.count_documents(
        {"dicom_tags.SOPInstanceUID.Value": {"$exists": True}}
    )
    logging.warning(
        f"with_SOPInstanceUID: {with_SOPInstanceUID},\n\
        with_SeriesInstanceUID:{with_SeriesInstanceUID},\n\
        with_StudyInstanceUID:{with_StudyInstanceUID}"
    )

    counts = list(
        im_dao.collection.aggregate(
            [
                {
                    "$group": {
                        "_id": "$dicom_tags.SOPInstanceUID.Value",
                        "image_count": {"$sum": 1},
                        "series_instance_uids": {
                            "$addToSet": {
                                "$first": "$dicom_tags.SeriesInstanceUID.Value"
                            }
                        },
                        "study_instance_uids": {
                            "$addToSet": {
                                "$first": "$dicom_tags.StudyInstanceUID.Value"
                            }
                        },
                    }
                },
                {
                    "$facet": {  # split the pipeline to count different things
                        "unique_SOPInstanceUID": [{"$count": "n"}],
                        "instances_with_more_than_1_image": [
                            {"$match": {"image_count": {"$gt": 1}}},
                            {"$count": "n"},
                        ],
                        "instances_with_more_than_1_series_uid": [
                            {"$match": {"series_instance_uids.1": {"$exists": True}}},
                            {"$count": "n"},
                        ],
                        "instances_with_more_than_1_study_uid": [
                            {"$match": {"study_instance_uids.1": {"$exists": True}}},
                            {"$count": "n"},
                        ],
                        "instances_with_more_than_1_series_and_study_uid": [
                            {"$match": {"study_instance_uids.1": {"$exists": True}}},
                            {"$count": "n"},
                        ],
                    }
                },
                {
                    "$project": {
                        "unique_SOPInstanceUID": {"$first": "$unique_SOPInstanceUID.n"},
                        "instances_with_more_than_1_image": {
                            "$first": "$instances_with_more_than_1_image.n"
                        },
                        "instances_with_more_than_1_series_uid": {
                            "$first": "$instances_with_more_than_1_series_uid.n"
                        },
                        "instances_with_more_than_1_study_uid": {
                            "$first": "$instances_with_more_than_1_study_uid.n"
                        },
                        "instances_with_more_than_1_series_and_study_uid": {
                            "$first": "$instances_with_more_than_1_series_and_study_uid.n"
                        },
                    }
                },
            ],
            allowDiskUse=True,
        )
    )[0]
    logging.warning(f"{counts}")


def cli():
    mount_paths, connector_kwargs, n_proc, project_name = parse_arguments(
        raw_args=sys.argv[1:]
    )
    main(mount_paths, connector_kwargs, n_proc, project_name)


if __name__ == "__main__":
    cli()
