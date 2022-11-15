from enum import Enum, IntFlag
from typing import Literal


class ByEnumName:
    """Marker to specify that this enum is stored by it's name in the db"""


class StrEnum(str, Enum):
    pass


class ProjectName(StrEnum):
    """Project name used in metadata as enum to keep it tidy"""

    VAIB = "vaib"
    MAMMOAI = "mammoai"


class Modality(StrEnum):
    """Modality of the image.
    See https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.3.html
    sect_C.7.3.1.1.1
    """

    MR = "MR"  # Magnetic Resonance
    MG = "MG"  # Mammography
    CR = "CR"  # Computed Radiography. Sometimes used instead of mammography MG
    CT = "CT"  # Computed Tomography
    SR = "SR"  # Structured Reporting
    US = "US"  # Ultrasound
    DX = "DX"  # Digital Radiography
    OTHER = "OT"  # Other
    PT = "PT"


class DataFormat(StrEnum):
    """
    Format of an image
    """

    DICOM = "dicom"
    JPEG = "jpeg"
    PNG = "png"
    TIFF = "tiff"


class Laterality(StrEnum):
    """Laterality of the image (0020,0060). Adding "bilateral" for MRIs that contain
    both breasts.
    """

    LEFT = "left"
    RIGHT = "right"
    BILATERAL = "bilateral"

    @classmethod
    def from_image_laterality(cls, image_laterality: str):
        if isinstance(image_laterality, str):
            if image_laterality.lower() == "l":
                return cls("left")
            elif image_laterality.lower() == "r":
                return cls("right")
        return None

    @classmethod
    def from_nkbc_a_pat_sida(cls, value: Literal[1, 2, "1", "2"]):
        """Map the description of a_pat_sida to left or right according to

        :param value: value coming from the NKBC database/csv. Literal[1, 2]
        :return: instance of this class
        """
        if value is None:
            return None
        if type(value) == str:
            value = int(value)
        if value == 1:
            return cls("right")
        if value == 2:
            return cls("left")
        if value == 3:
            return cls("bilateral")
        raise ValueError(
            f"Value {value} for a_pat_sida could not be mapped to 'right' or 'left'"
        )


class View(StrEnum):
    """
    View of the image.
    """

    AX = "axial"
    SAG = "sagittal"
    COR = "coronal"
    MLO = "mediolateral_oblique"
    CC = "craniocaudal"

    @classmethod
    def from_view_position(cls, view_position: str):
        if isinstance(view_position, str):
            if view_position in ["AX", "SAG", "COR", "MLO", "CC"]:
                return getattr(cls, view_position)
        return None


class PregnancyStatus(StrEnum):
    """
    Pregnancy status of the patient. (0010,21C0).
    """

    NOT_PREGNANT = "0001"
    POSSIBLY_PREGNANT = "0002"
    DEFINETLY_PREGNANT = "0003"
    UNKNOWN = "0004"


class PatientPosition(StrEnum):
    """Patient Position
    https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.3.html
    sect_C.7.3.1.1.2
    """

    HFP = "HFP"  # "Head First-Prone"
    HFS = "HFS"  # "Head First-Supine"
    HFDR = "HFDR"  # "Head First-Decubitus Right"
    HFDL = "HFDL"  # "Head First-Decubitus Left"
    FFDR = "FFDR"  # "Feet First-Decubitus Right"
    FFDL = "FFDL"  # "Feet First-Decubitus Left"
    FFP = "FFP"  # "Feet First-Prone"
    FFS = "FFS"  # "Feet First-Supine"
    LFP = "LFP"  # "Left First-Prone"
    LFS = "LFS"  # "Left First-Supine"
    RFP = "RFP"  # "Right First-Prone"
    RFS = "RFS"  # "Right First-Supi"
    AFDR = "AFDR"  # "Anterior First-Decubitus Right"
    AFDL = "AFDL"  # "Anterior First-Decubitus Left"
    PFDR = "PFDR"  # "Posterior First-Decubitus Right"
    PFDL = "PFDL"  # "Posterior First-Decubitus Left"


class PersonalNumberValidity(IntFlag):
    """Describes how valid a Swedish Personal Number is.
    As a binary set of flags where 1 means valid.
    0b |0        |0    |0      |0
       |checksum |date |length |chars
    In the end all the flags are interpreted as an integer. For example:
    3 would be 0b0011 which means valid chars and length.
    6 would be 0b0110 which means valid length and date.
    7 would be 0b0111 which means only the checksum is not valid
    And so on for any number between 0 and 15
    """

    NOT_VALID = 0
    CHARS = 1  # only 0-9, a-z, or A-Z characters. No other dashes dots slashes etc.
    LENGTH = 2  # 12 characters YYYYMMDDXXXX (after removing other characters that are
    # not 0-9 a-z A-Z)
    DATE = 4  # A date can be parsed (when using only YYMMDD)
    CHECKSUM = 8  # The checksum is correct
    VALID = 15  # everything is correct
