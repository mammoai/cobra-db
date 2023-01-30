import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import List, Union

import pydicom
from deid.config import DeidRecipe
from deid.dicom.parser import DicomParser
from deid.logger import bot

from cobra_db.encrypt import Hasher
from cobra_db.utils import parse_AS_as_int

base_recipe_path = os.path.join(os.path.dirname(__file__), "deid_recipe.txt")
mr_recipe_path = os.path.join(os.path.dirname(__file__), "deid_recipe_mr.txt")

deid_logging_levels = dict(
    ABORT=-5,
    FLAG=-4,
    ERROR=-3,
    WARNING=-2,
    LOG=-1,
    INFO=1,
    CUSTOM=1,
    QUIET=0,
    VERBOSE=2,
    VERBOSE2=3,
    VERBOSE3=4,
    DEBUG=5,
)


class Deider:
    def __init__(
        self,
        hasher_secret_salt: str,
        recipe_path: Union[str, List] = None,
        logging_level: str = "ERROR",
    ):
        """Deidentify datasets according to vaib recipe

        :param recipe_path: path to the deid recipe
        :param hasher_secret_salt: salt for hashing
        """
        bot.level = deid_logging_levels[logging_level]
        if recipe_path is None:
            logging.warning(f"DeidDataset using default recipe {base_recipe_path}")
            recipe_path = base_recipe_path
        if type(recipe_path) == list:
            for r in recipe_path:
                assert os.path.exists(r), f"Invalid recipe_path: {r}"
        else:
            assert os.path.exists(recipe_path), f"Invalid recipe_path: {recipe_path}"
        self.recipe = DeidRecipe(recipe_path)
        self.hasher = Hasher(hasher_secret_salt)

    def pseudonymize(self, dataset: pydicom.Dataset) -> pydicom.Dataset:
        """Pseudonymize a single dicom dataset

        :param dataset: dataset that will be pseudonymized
        :returns: pseudonymized dataset
        """
        dataset = deepcopy(dataset)
        parser = DicomParser(dataset, self.recipe)
        parser.define("replace_name", self._replace_name)
        parser.define("hash_func", self._deid_hash_func)
        parser.define("remove_day", self._remove_day)
        parser.define("round_AS_to_nearest_5y", self._round_AS_to_nearest_5y)
        parser.define("round_DS_to_nearest_5", self._round_DS_to_nearest_5)
        parser.define("round_DS_to_nearest_0_05", self._round_DS_to_nearest_0_05)
        parser.parse(strip_sequences=True, remove_private=True)
        return parser.dicom

    @staticmethod
    def _remove_day(item, value, field, dicom):
        """Removes the day from a DT field in the deid framework"""
        date = field.element.value
        if date == "":
            return ""
        dt = datetime.strptime(date, "%Y%m%d")
        return dt.strftime("%Y%m01")

    @staticmethod
    def _replace_name(item, value, field, dicom):
        sex = dicom.get("PatientSex")
        sex = {"F": "Female", "M": "Male", "O": "Other", "": "Unk"}[sex]
        age = Deider._round_to_nearest(parse_AS_as_int(dicom.get("PatientAge")), 5)
        return f"{sex} {age:03d}Y {dicom.get('Modality')}"

    @staticmethod
    def _round_to_nearest(value, interval):
        """Rounds value to closest multiple of interval"""
        return interval * round(value / interval)

    @staticmethod
    def _round_AS_to_nearest_5y(item, value, field, dicom):
        """Rounds age(AS) field to 5 year intervals in the deid framework"""
        age = parse_AS_as_int(field.element.value)
        return f"{Deider._round_to_nearest(age, 5):03d}Y"

    @staticmethod
    def _round_DS_to_nearest_5(item, value, field, dicom):
        """Rounds age(AS) field to 5 year intervals in the deid framework"""
        return f"{Deider._round_to_nearest(float(field.element.value), 5)}"

    @staticmethod
    def _round_DS_to_nearest_0_05(item, value, field, dicom) -> str:
        """Rounds field.element.value to increments of 0.05"""
        value = field.element.value
        if value is None:
            value = -1
        return f"{Deider._round_to_nearest(float(value), 0.05):.02f}"

    def _deid_hash_func(self, item, value, field, dicom) -> str:
        """Performs self.hash to field.element.value"""
        val = field.element.value
        return self.hasher.hash(str(val))
