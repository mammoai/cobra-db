import sys
from argparse import ArgumentParser
from typing import List, Tuple

from cobra_db.scripts.stage_2_group_patients import main as group_patients
from cobra_db.scripts.stage_2_group_patients import (
    main_multiproc as group_patients_multiproc,
)
from cobra_db.scripts.stage_2_group_series import main as group_series
from cobra_db.scripts.stage_2_group_series import (
    main_multiproc as group_series_multiproc,
)
from cobra_db.scripts.stage_2_group_studies import main as group_studies
from cobra_db.scripts.stage_2_group_studies import (
    main_multiproc as group_studies_multiproc,
)


def parse_arguments(raw_args: List[str]) -> Tuple[str, dict, int]:
    """Convert the console arguments into python variables.

    :param raw_args: the list of commands (usually sys.argv[1:])
    :return: connector_kwargs, n_proc
    """
    parser = ArgumentParser(
        description="Once the ImageMetadata collection is populated, group the images \
        into series, studies and patients."
    )
    parser.add_argument("-ho", "--host", help="MongoDB host", default="127.0.0.1")
    parser.add_argument("-p", "--port", help="MongoDB port", default=27017, type=int)
    parser.add_argument("-u", "--username", help="MongoDB username", default=None)
    parser.add_argument(
        "-db", "--database_name", help="MongoDB database", required=True
    )
    parser.add_argument(
        "-n", "--num_processes", help="Number of processes to use", default=1, type=int
    )
    parser.add_argument(
        "--project_name",
        help="The name of the project that gets stored in the _metadata field",
    )

    args = parser.parse_args(raw_args)
    connector_kwargs = {
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "db_name": args.database_name,
    }
    return connector_kwargs, args.num_processes, args.project_name


def main(connector_kwargs: dict, n_proc: int, project_name: str):
    if n_proc == 1:
        group_series(connector_kwargs, project_name)
        group_studies(connector_kwargs, project_name)
        group_patients(connector_kwargs)  # patients dont have a project.
    else:
        group_series_multiproc(connector_kwargs, n_proc, project_name)
        group_studies_multiproc(connector_kwargs, n_proc, project_name)
        group_patients_multiproc(connector_kwargs, n_proc)


def cli():
    connector_kwargs, n_proc, project_name = parse_arguments(sys.argv[1:])
    main(connector_kwargs, n_proc, project_name)
