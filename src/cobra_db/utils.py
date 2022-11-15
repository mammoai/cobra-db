import logging
import os
import subprocess
from datetime import datetime
from typing import List


def find_files(path: str, query: str, regex: bool = False):
    """Lazyly use the "find" bash command to recursively get all the files
    that match query on path.

    :param path: path to search
    :param query: query to match
    :param regex: use -regex instead of -name
    :raises FileNotFoundError: when nothing is found
    :yield: absolute paths of files
    """
    flag = "-regex" if regex else "-name"
    process = subprocess.Popen(
        ["find", path, "-type", "f", flag, query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for c in iter(process.stdout.readline, b""):
        c = c.decode().rstrip("\n")
        yield c
    if (e := process.stderr.readline().decode().rstrip("\n")) != "":
        raise FileNotFoundError(e)


def find_dcm(path):
    """Lazy iterator to recursively find all files with .dcm extension under a path

    :param path: path to find
    :yield: absolute paths of files
    """
    for c in find_files(path, "*.dcm"):
        yield c


def list_dirs(path: str) -> List[str]:
    """List all directories directly below path

    :param path: path to get directories from
    :return: list of directories as absolute paths
    """
    dirs = [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
    return sorted([os.path.join(path, d) for d in dirs])


def list_files(folder_path: str, extension: str) -> List[str]:
    """List paths of files with <extension> directly inside a folder.

    :param folder_path: the path to the folder
    :param extension: the extension to look for
    :return: list of absolute paths of files
    """
    filenames = []
    extension = f".{extension}" if not extension.startswith(".") else extension
    for f in os.listdir(folder_path):
        if os.path.splitext(f)[1] == extension:
            filenames.append(f)
    return sorted([os.path.join(folder_path, f) for f in filenames])


def parse_DA_TM_as_datetime(DA: str, TM: str) -> datetime:
    """
    Parse DA and TM as datetime
    """
    if DA == "00000000" and TM == "000000":
        return None
    if "." in TM:
        format = "%Y%m%d %H%M%S.%f"
    else:
        format = "%Y%m%d %H%M%S"
    return datetime.strptime(f"{DA} {TM}", format)


def parse_AS_as_int(AS: str) -> int:
    """
    Parse AS as int
    """
    if AS is None:
        logging.warning("AS is empty, returning -1")
        return -1
    if len(AS) > 0:
        if AS[-1] == "Y":
            return int(AS[:-1])
        else:
            raise ValueError(f"AS {AS} is not in years")
    else:
        logging.warning("Not skiping but AS is empty")
        return -1


def parse_DA_as_datetime(DA: str):
    return datetime.strptime(DA, "%Y%m%d")


def intersect_dicts(dicts: List[dict]) -> dict:
    """
    Reads the fist level keys and returns everything that is the same for all dicts.
    """
    ans = dict()
    for k, v in dicts[0].items():
        add_pair = True
        v_s = str(v)
        for d in dicts[1:]:
            # Case key does not exist
            d_v = d.get(k, None)
            if d_v is None:
                add_pair = False
                break
            else:  # Case key exists but values are different
                d_v = str(d_v)
                if d_v != v_s:
                    add_pair = False
                    break
        if add_pair:
            ans[k] = v
    return ans


def intersect_dicts_allow_empty_minority(dicts: List[dict]) -> dict:
    """
    Reads first level keys and returns a dict with all the keys where the values are
    equal.
    If the key does not exist in less than half, but all the other dicts that contain it
    agree on the value, then the key is kept.
    """
    # get a set of all the possible keys
    all_keys = set()
    ans = dict()
    n_dicts = len(dicts)
    majority = (n_dicts // 2) + 1
    for d in dicts:
        for k in d.keys():
            all_keys.add(k)
    for k in sorted(all_keys):
        add_key = True
        # get values for k of all dicts if it exists
        values = [d[k] for d in dicts if d.get(k, None) is not None]
        n_values = len(values)
        if n_values < majority:
            add_key = False
        if n_values > 1:
            str_v0 = str(values[0])
            for v in values[1:]:
                if str_v0 != str(v):
                    add_key = False
                    break
        if n_values == 0:  # in case key existed but had a None
            add_key = False
        if add_key:
            ans[k] = values[0]
    return ans
