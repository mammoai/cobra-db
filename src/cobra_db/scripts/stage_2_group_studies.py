# In stage 2 we create the hierarchy of files from the metadata.
# First, we group files by PatientID+StudyDate

import logging
from multiprocessing import Pool
from typing import List, Tuple

from pydicom import Dataset
from tqdm import tqdm

from cobra_db.dataset_mod import DatasetMod
from cobra_db.model import RadiologicalStudy
from cobra_db.mongo_dao import Connector, ImageMetadataDao, SeriesDao, StudyDao
from cobra_db.scripts.utils import add_args_to_iterable, batcher
from cobra_db.utils import intersect_dicts_allow_empty_minority


def proto_study_generator(connector_kwargs):
    query = [
        {
            "$group": {
                "_id": {
                    "patient_id": {"$first": "$dicom_tags.PatientID.Value"},
                    "study_date": {"$first": "$dicom_tags.StudyDate.Value"},
                },
                "image_ids": {"$push": "$_id"},
            }
        }
    ]
    connector = Connector.get_env_pass(**connector_kwargs)
    im_dao = ImageMetadataDao(connector)
    proto_study_cursor = im_dao.collection.aggregate(query, allowDiskUse=True)
    # Store it in a list otherwise the cursor might die after ~30min
    proto_study_list = list(proto_study_cursor)
    for i in proto_study_list:
        yield i


def get_tag(d, keyword):
    try:
        return d[keyword]["Value"][0]
    except KeyError:
        return None


def group_study(images: List[dict], project_name: str) -> RadiologicalStudy:
    """Takes the dict of many ImageMetadata instances.
    Obtains the shared dicom_tags and extract more information to put it in the root
    of the document.

    :param images: List of dictionaries from the ImageMetadata collection
    :param project_name: Project to which this study belongs.
    """
    dicom_tags = [i["dicom_tags"] for i in images]
    intersection = intersect_dicts_allow_empty_minority(dicom_tags)
    shared_ds_dict = DatasetMod.keywords_to_tags(intersection)
    shared_ds = Dataset.from_json(shared_ds_dict)
    series_uids = set([get_tag(x, "SeriesInstanceUID") for x in dicom_tags])
    study_uids = set([get_tag(x, "StudyInstanceUID") for x in dicom_tags])
    accession_numbers = set([get_tag(x, "AccessionNumber") for x in dicom_tags])
    try:
        study = RadiologicalStudy.from_dataset(shared_ds)
    except ValueError as e:
        logging.error(
            f"Could not create study instance for \
    {get_tag(dicom_tags[0], 'PatientID')}-{get_tag(dicom_tags[0], 'StudyDate')}, e:{e}"
        )
        return None
    study.series_count = len(series_uids)
    study.modality = list(
        set([get_tag(x, "Modality") for x in dicom_tags if get_tag(x, "Modality")])
    )
    study.sop_class = list(
        set(
            [get_tag(x, "SOPClassUID") for x in dicom_tags if get_tag(x, "SOPClassUID")]
        )
    )
    study.study_uid = list(study_uids)
    study.accession_number = list(accession_numbers)
    study._metadata.project_name = project_name
    return study


def process_study_batch(args: Tuple[List[dict], dict, str]):
    batch, connector_kwargs, project_name = args
    connector = Connector.get_env_pass(**connector_kwargs)
    im_dao = ImageMetadataDao(connector)
    study_dao = StudyDao(connector)
    series_dao = SeriesDao(connector)
    counter = 0
    for study in batch:
        images = [im_dao.get_by_id(i, obj=False) for i in study["image_ids"]]
        study = group_study(images, project_name)
        if study is None:
            continue
        study_id = study_dao.insert_one(study)
        updated_series = set()
        for i in images:
            im_dao.update_study_id(i["_id"], study_id)
            series_id = i.get("series_id")
            if series_id and series_id not in updated_series:
                series_dao.update_study_id(series_id, study_id)
                updated_series.add(series_id)
        counter += 1
    return counter


def main(connector_kwargs, project_name):
    iterable = batcher(proto_study_generator(connector_kwargs), 5)
    iterable = add_args_to_iterable(iterable, connector_kwargs, project_name)
    pbar = tqdm(desc="studies", smoothing=0.05)
    for batch in iterable:
        n = process_study_batch(batch)
        pbar.update(n)


def main_multiproc(connector_kwargs, n_proc, project):
    pool = Pool(n_proc)
    iterable = batcher(proto_study_generator(connector_kwargs), 5)
    iterable = add_args_to_iterable(iterable, connector_kwargs, project)
    pbar = tqdm(desc="studies", smoothing=0.005)
    for n in pool.imap_unordered(process_study_batch, iterable, chunksize=1):
        pbar.update(n)
