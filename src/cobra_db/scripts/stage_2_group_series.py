# In stage 2 we create the hierarchy of files from the metadata.
# First, we group files by SeriesInstanceUID

import logging
from multiprocessing import Pool
from typing import List, Tuple

from pydicom.dataset import Dataset
from tqdm import tqdm

from cobra_db.dataset_mod import DatasetMod
from cobra_db.model import RadiologicalSeries
from cobra_db.mongo_dao import Connector, ImageMetadataDao, SeriesDao
from cobra_db.scripts.utils import add_args_to_iterable, batcher
from cobra_db.utils import intersect_dicts_allow_empty_minority


def proto_series_generator(connector_kwargs):
    query = [
        {"$match": {"dicom_tags.SeriesInstanceUID.Value": {"$exists": True}}},
        # {
        #     "$limit": 50000
        # },
        {
            "$group": {
                "_id": {"$first": "$dicom_tags.SeriesInstanceUID.Value"},
                "instance_ids": {"$push": "$_id"},
                "study_uids": {"$first": "$dicom_tags.StudyInstanceUID.Value"},
                "patient_anon_ids": {"$first": "$dicom_tags.PatientID.Value"},
            }
        },
    ]
    connector = Connector.get_env_pass(**connector_kwargs)
    dao = ImageMetadataDao(connector)
    series_cursor = dao.collection.aggregate(query, allowDiskUse=True)
    for i in series_cursor:
        yield i


def process_series(
    proto_series: dict,
    im_dao: ImageMetadataDao,
    series_dao: SeriesDao,
    project_name: str,
):
    dicom_tags = [
        im_dao.get_by_id(i, obj=False)["dicom_tags"]
        for i in proto_series["instance_ids"]
    ]
    proto_series["dicom_tags"] = intersect_dicts_allow_empty_minority(dicom_tags)
    shared_ds_dict = DatasetMod.keywords_to_tags(proto_series["dicom_tags"])
    shared_ds = Dataset.from_json(shared_ds_dict)
    series = RadiologicalSeries.from_dataset(shared_ds)
    series._metadata.project_name = project_name
    series.image_count = len(proto_series["instance_ids"])  # tags are not trustable
    series_id = series_dao.insert_one(series)
    for i in proto_series["instance_ids"]:
        im_dao.update_series_id(i, series_id)


def process_series_batch(args: Tuple[List[dict], dict]):
    batch, connector_kwargs, project_name = args
    connector = Connector.get_env_pass(**connector_kwargs)
    im_meta_dao = ImageMetadataDao(connector)
    series_dao = SeriesDao(connector)
    counter = 0
    for proto_series in batch:
        try:
            process_series(proto_series, im_meta_dao, series_dao, project_name)
        except Exception as e:
            logging.error(f"SeriesUID {proto_series['_id']} - {e}")
        counter += 1
    connector.client.close()
    return counter


def sanity_check(connector_kwargs):
    connector = Connector.get_env_pass(**connector_kwargs)
    im_dao = ImageMetadataDao(connector)
    total_docs = im_dao.collection.count_documents({})
    docs_missing_series_uid = im_dao.collection.count_documents(
        {"dicom_tags.SeriesInstanceUID.Value": {"$exists": False}}
    )
    logging.warning(
        f"Total images: {total_docs}. Missing SeriesInstanceUID tag:\
        {docs_missing_series_uid} \
        ({round(docs_missing_series_uid*100/total_docs, 4)}%)"
    )


def main_multiproc(connector_kwargs, n_proc, project_name):
    # sanity check before starting
    sanity_check(connector_kwargs)
    pool = Pool(n_proc)
    iterable = batcher(proto_series_generator(connector_kwargs), 5)
    iterable = add_args_to_iterable(iterable, connector_kwargs, project_name)
    pbar = tqdm(desc="series", smoothing=0.005)
    for n in pool.imap_unordered(process_series_batch, iterable, chunksize=1):
        pbar.update(n)


def main(connector_kwargs, project_name):
    sanity_check(connector_kwargs)
    connector = Connector.get_env_pass(**connector_kwargs)
    series_dao = SeriesDao(connector)
    im_dao = ImageMetadataDao(connector)
    iterable = proto_series_generator(connector_kwargs)
    pbar = tqdm(desc="series", smoothing=0.005)
    for proto_series in iterable:
        try:
            process_series(proto_series, im_dao, series_dao, project_name)
            pbar.update()
        except Exception as e:
            logging.error(f"SeriesUID {proto_series['_id']} - {e}")


# if __name__ == "__main__":
#      # configure logging
#     now = datetime.now().strftime("%Y%m%d_%H%M")
#     log_path = f"stage2_group_series_{now}.log"
#     logging.basicConfig(
#         filename=log_path,
#         level=logging.WARNING,
#         format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#     )
#     connector_kwargs = dict(
#         host="127.0.0.1",
#         port=27027,
#         username="fercos",
#         db_name="coma_original"
#     )
#     main_multiproc(connector_kwargs)
