import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool
from typing import List, Union

import pydicom
from bson import ObjectId
from pyaml_env import parse_config
from pydicom.errors import BytesLengthException
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

from cobra_db.dataset_mod import DatasetMod
from cobra_db.deid import Deider, base_recipe_path, mr_recipe_path
from cobra_db.filesystem import get_instance_path
from cobra_db.model import FileSource, ImageMetadata
from cobra_db.mongo_dao import Connector, ImageMetadataDao
from cobra_db.scripts.utils import add_args_to_iterable, batcher


def parse_arguments(raw_args: List[str]) -> str:
    """Parse the arguments for the pseudonymization process

    :param raw_args: arguments from sys.argv[1:]
    :return: a namespace with the required cfg
    """
    parser = ArgumentParser(
        "Pseudonymize ImageMetadata",
        description="""
Given an ImageMetadata collection with real data, create a second database with the
pseudonymized version. The files will be pseudonymized and stored according to the
suggested filesystem paths. Additionally, docs that are correctly pseudonymized will
be updated with a `deid_file_source` field that allows to check where the data went.""",
    )

    parser.add_argument(
        "config_path",
        type=str,
        help="the config file to be used.\
        For an example of the config file, please read the documentation how-to guide",
    )

    args = parser.parse_args(raw_args)
    return Namespace(**parse_config(args.config_path, default_value=None))


def im_meta_generator(src_mongo: dict, query: dict = {}):
    """Generator of im_meta dicts

    :param src_mongo: kwargs for the Connector
    :param query: query to select subset of the ImageMetadata collection
    :yield: dict with image metadata.
    """

    connector = Connector(**src_mongo)
    im_dao = ImageMetadataDao(connector)
    try:
        logging.info("Starting generator of ImageMetadata")
        cursor = im_dao.collection.find(
            query,
            {
                "_id": True,
                "_metadata": True,
                "dicom_tags": {"Not used": "Not used"},
                "file_source": True,
                "study_id": True,
                "series_id": True,
                "aka_file_sources": True,
            },
        )
        for x in cursor:
            yield x
    finally:
        cursor.close()


def im_meta_from_virtual_dataset(
    ds: pydicom.Dataset, file_source: FileSource, _id: ObjectId = None
):
    """An alternative version of ImageMetadata.from_dataset that does not require a
    ds.filename

    :param ds: Dicom dataset
    :param file_source: Source of the dataset
    :param _id: the id that will be stored in the dst_mongo
    """
    json_dict = ds.to_json_dict(suppress_invalid_tags=True)
    dicom_tags = DatasetMod.tags_to_keywords(json_dict)
    return ImageMetadata(
        _id=_id,
        series_id=None,
        _metadata=None,
        file_source=file_source,
        dicom_tags=dicom_tags,
    )


def process_batch(args):
    im_metas, cfg = args

    src_im_dao = ImageMetadataDao(Connector(**cfg.src_mongo))
    dst_im_dao = ImageMetadataDao(Connector(**cfg.dst_mongo))
    try:
        processed = 0
        seen = 0
        for im_meta in im_metas:
            seen += 1
            try:
                processed += process_im_meta(im_meta, cfg, src_im_dao, dst_im_dao)
            except IndexError:
                pass
    finally:
        src_im_dao.connector.close()
        dst_im_dao.connector.close()

    return seen, processed


def process_im_meta(
    im_meta_dict: dict, cfg, src_im_dao: ImageMetadataDao, dst_im_dao: ImageMetadataDao
):
    im_meta = ImageMetadata.from_dict(im_meta_dict)
    # get source file
    src_filepath = im_meta.get_local_filepath(cfg.mount_paths)
    try:
        src_ds = pydicom.read_file(src_filepath)
    except Exception as e:
        logging.error(f"Could not read {im_meta._id} - {e}")
        return 0

    # pseudonymize
    try:
        deid_ds = cfg.deider.pseudonymize(src_ds)
    except ValueError as e:
        logging.error(f"Could not pseudonymize {im_meta._id} - {e}")
        return 0
    except KeyError as e:
        logging.error(f"Could not pseudonymize {im_meta._id} - {e}")
        return 0
    except BytesLengthException as e:
        logging.error(f"Could not pseudonymize {im_meta._id} - {e}")
        return 0

    # get path where it will be saved
    dst_rel_dir = cfg.dst_rel_dir
    if dst_rel_dir is None:
        dst_rel_dir = ""
    dst_rel_filepath = os.path.join(dst_rel_dir, get_instance_path(deid_ds))
    dst_file_source = FileSource(cfg.dst_drive_name, dst_rel_filepath)
    if dst_file_source.drive_name in [
        x.get("drive_name") for x in im_meta_dict.get("aka_file_sources", [])
    ]:
        logging.debug("This file has already been created.")
        raise IndexError("This file has already been created.")
    dst_filepath = dst_file_source.get_local_filepath(cfg.mount_paths)

    os.makedirs(os.path.dirname(dst_filepath), exist_ok=True)
    try:
        pydicom.write_file(dst_filepath, deid_ds)
    except ValueError as e:
        logging.error(f"{e}")

    # the _id of the dataset is stored with the same database _id so that a super user
    # with access to both collections, can quickly check that everything is okay.
    try:
        del deid_ds.PixelData
    except AttributeError as e:
        logging.error(f"{e}")

    deid_im_meta = im_meta_from_virtual_dataset(deid_ds, dst_file_source, im_meta._id)

    # Keep record of what happened
    try:
        dst_im_dao.insert_one(deid_im_meta)
    except DuplicateKeyError as e:
        logging.error(f"{e}")
        return 0
    src_im_dao.add_aka(im_meta._id, dst_file_source)
    return 1


def get_required_drive_names(src_mongo, query):
    connector = Connector(**src_mongo)
    im_dao = ImageMetadataDao(connector)
    logging.info("Searching images to obtain required drives for query")
    cursor = im_dao.collection.aggregate(
        [{"$match": query}, {"$sortByCount": "$file_source.drive_name"}]
    )
    drive_names = []
    images = 0
    for doc in cursor:
        drive_names.append(doc["_id"])
        images += doc["count"]
    return drive_names, images


def check_mount_paths(mount_paths, required_drive_names, dst_drive_name):
    missing = set(required_drive_names) - set(mount_paths.keys())
    assert missing == set(), f"Missing configuration for drive_names: {missing}"
    assert mount_paths.get(dst_drive_name) is not None, "Missing dst_drive_name"


def single_proc(batches, n_proc=1):
    for batch in batches:
        seen, processed = process_batch(batch)
        yield seen, processed


def multi_proc(batches, n_proc):
    pool = Pool(n_proc)
    for seen, processed in pool.imap_unordered(process_batch, batches):
        yield seen, processed


def query_mux(query, image_ids: List[ObjectId] = None):
    """Select one of query or image_ids"""
    if image_ids is not None:
        assert query == {}, "Cannot have a query and a list of images at the same time"
        query = {"_id": {"$in": image_ids}}
    return query


def recipe_mux(base: bool, mr: bool, recipe: Union[str, List[str]]):
    """Select the correct recipe according to the configurations"""
    recipes = []
    if base:
        print(f"Using VAIB recipe from: {base_recipe_path}")
        recipes.append(base_recipe_path)
    if mr:
        print(
            f"Using additional to VAIB recipe targeted to MR studies from: {mr_recipe_path}"
        )
        recipes.append(mr_recipe_path)
    if type(recipe) == str:
        recipes.append(recipe)
    if type(recipe) == list:
        recipes = recipes + recipe
    for r in recipes:
        assert os.path.exists(r), f"Deid recipe path does not exist: {r}"
    if len(recipes) == 0:
        return None
    logging.info(f"Using recipes: {recipes}")
    return recipes


def main(args=None, image_ids: List[ObjectId] = None):
    if args is None:
        # Allows testing main from pytest.
        args = sys.argv[1:]

    # read config file
    cfg = parse_arguments(args)
    cfg.query = query_mux(cfg.query, image_ids)

    # Check how many files will be processed and if the cfg is enough
    required_drive_names, total_imgs = get_required_drive_names(
        cfg.src_mongo, cfg.query
    )
    check_mount_paths(cfg.mount_paths, required_drive_names, cfg.dst_drive_name)
    im_meta_gen = im_meta_generator(cfg.src_mongo, cfg.query)
    logging.info("Saving list of files to be processed")
    imgs = list(im_meta_gen)  # save as list to avoid losing the
    batches = batcher(imgs, batch_size=cfg.batch_size)

    # Due to a weird configuration system of the deid library this has to be loaded
    # after setting the MESSAGELEVEL env var.
    os.environ["MESSAGELEVEL"] = cfg.logging_level

    cfg.deider = Deider(
        cfg.hash_secret,
        recipe_mux(
            cfg.deid_default_recipes["base"],
            cfg.deid_default_recipes["mr"],
            cfg.user_recipe_path,
        ),
        logging_level=cfg.logging_level,
    )

    batches = add_args_to_iterable(batches, cfg)
    pbar_seen = tqdm(total=total_imgs, desc="Images seen")
    pbar_processed = tqdm(total=total_imgs, desc="Images processed")
    if cfg.n_proc == 1:
        process_func = single_proc
    if cfg.n_proc > 1:
        process_func = multi_proc
    if cfg.n_proc < 1:
        raise ValueError("n_proc must be bigger than 0")
    for seen, processed in process_func(batches, cfg.n_proc):
        pbar_processed.update(processed)
        pbar_seen.update(seen)
