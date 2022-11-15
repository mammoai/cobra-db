from multiprocessing import Pool
from typing import List, Tuple

from pydicom import Dataset
from tqdm import tqdm

from cobra_db.dataset_mod import DatasetMod
from cobra_db.model import Patient
from cobra_db.mongo_dao import Connector, PatientDao, StudyDao
from cobra_db.scripts.utils import add_args_to_iterable, batcher
from cobra_db.utils import intersect_dicts_allow_empty_minority


def proto_patient_generator(connector_kwargs):
    pipeline = [
        {
            "$group": {
                "_id": {"$first": "$dicom_tags.PatientID.Value"},
                "study_ids": {"$push": "$_id"},
            }
        }
    ]

    connector = Connector.get_env_pass(**connector_kwargs)
    study_dao = StudyDao(connector)
    study_cursor = study_dao.collection.aggregate(pipeline, allowDiskUse=True)
    for i in study_cursor:
        yield i


def process_patient_batch(args: Tuple[List[dict], dict]):
    batch, connector_kwargs = args
    connector = Connector.get_env_pass(**connector_kwargs)
    study_dao = StudyDao(connector)
    patient_dao = PatientDao(connector)

    counter = 0

    for patient in batch:
        studies = [study_dao.get_by_id(i, obj=False) for i in patient["study_ids"]]
        dicom_tags = [s["dicom_tags"] for s in studies]
        intersection = intersect_dicts_allow_empty_minority(dicom_tags)
        shared_ds_dict = DatasetMod.keywords_to_tags(intersection)
        shared_ds = Dataset.from_json(shared_ds_dict)
        patient = Patient.from_dataset(shared_ds)
        patient_id = patient_dao.insert_one(patient)
        counter += 1
        for study in studies:
            study_dao.update_patient_id(study["_id"], patient_id)
    return counter


def main(connector_kwargs):
    batch_size = 5
    iterable = batcher(proto_patient_generator(connector_kwargs), batch_size)
    iterable = add_args_to_iterable(iterable, connector_kwargs)
    pbar = tqdm(desc="patients", smoothing=0.005)
    for batch in iterable:
        counter = process_patient_batch(batch)
        pbar.update(counter)


def main_multiproc(connector_kwargs, n_proc):
    pool = Pool(n_proc)
    iterable = batcher(proto_patient_generator(connector_kwargs), 5)
    iterable = add_args_to_iterable(iterable, connector_kwargs)
    pbar = tqdm(desc="patients", smoothing=0.005)
    for n in pool.imap_unordered(process_patient_batch, iterable, chunksize=1):
        pbar.update(n)
