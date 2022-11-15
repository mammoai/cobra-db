from typing import Callable

from pydicom.datadict import keyword_for_tag, tag_for_keyword


class DatasetMod:
    """A functional class to replace the tags with keywords and viceversa in the output
    of pydicom.dataset.Dataset.to_json_dict
    """

    @staticmethod
    def tag_for_keyword_wrapper(keyword: str) -> str:
        """wrapper to get strings instead of ints"""
        if keyword.startswith("60") or keyword.startswith(
            "50"
        ):  # return the keyword as is
            return keyword
        tag_int = tag_for_keyword(keyword)
        if tag_int is not None:
            return f"{tag_int:08x}"
        else:  # There is no Keyword for the tag
            return keyword

    @staticmethod
    def keyword_for_tag_wrapper(tag: str) -> str:
        """Ignore translation of Repeating Groups. See
        https://pydicom.github.io/pydicom/dev/old/working_with_overlays.html
        """
        keyword = keyword_for_tag(tag)
        if (
            tag.startswith("60")
            or tag.startswith("50")
            or keyword.startswith("Overlay")
        ):
            return tag
        if keyword == "":  # if the tag is unknown, then leave the tag
            return tag
        return keyword

    @staticmethod
    def _replace_dict(d: dict, replace_func: Callable) -> dict:
        """d: a posibly nested dict where all tag keys are converted into keywords"""
        ans = dict()
        for old_k, v in d.items():
            new_k = replace_func(old_k)
            # check if value is nested
            if v["vr"] == "SQ":
                values = v.pop("Value", None)
                if values is not None:
                    v["Value"] = [
                        DatasetMod._replace_dict(seq_obj, replace_func)
                        for seq_obj in values
                    ]
            ans[new_k] = v
        return ans

    @staticmethod
    def tags_to_keywords(d: dict) -> dict:
        return DatasetMod._replace_dict(d, DatasetMod.keyword_for_tag_wrapper)

    @staticmethod
    def keywords_to_tags(d: dict) -> dict:
        return DatasetMod._replace_dict(d, DatasetMod.tag_for_keyword_wrapper)
