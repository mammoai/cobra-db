import re
from datetime import datetime
from typing import Tuple

from Crypto.Hash import SHA512

from cobra_db.enums import PersonalNumberValidity


class PersonalNumberValidator:
    """Class to group all methods related to validating personal numbers.
    Checksum is calculated according to
    https://en.wikipedia.org/wiki/Personal_identity_number_(Sweden)#Checksum
    """

    @staticmethod
    def get_checksum(personal_number: str) -> int:
        """Calculate the checksum for a personal number

        :param personal_number: 10 digit pnr. Without any other characters.
        """
        identity = "212121212"
        multiply = [str(int(p) * int(i)) for p, i in zip(personal_number, identity)]
        sum_digits = str(sum(int(m) for m in "".join(multiply)))
        last_digit = sum_digits[-1]
        return 10 - int(last_digit)

    @staticmethod
    def validate(personal_number: str) -> PersonalNumberValidity:
        """
        Checks different aspects of validity described in the PnrValidity enum.
        It is important to store this information along with the hashed PersonalNumber
        to be able to debug matching problems later. This is because the hashed version
        of 19000101-1234, 190001011234 and 0001011234 are totally different.
        At least with this information we can try to modify one side or the other of the
        matching to fix it.
        """
        validity = PersonalNumberValidity.NOT_VALID
        # check for characters outside 0-9
        p = personal_number
        personal_number = re.sub(r"[^\da-zA-Z]+", "", personal_number)
        if p == personal_number:
            validity = validity | PersonalNumberValidity.CHARS

        if len(personal_number) == 10:
            pass
        elif len(personal_number) == 12:  # long version of the personnummer
            validity = validity | PersonalNumberValidity.LENGTH
            # make it short since the other two digits are not used in the checksum
            personal_number = personal_number[2:]

        try:  # parse the date to and raise errors if something wrong
            datetime.strptime(personal_number[:6], "%y%m%d")
            validity = validity | PersonalNumberValidity.DATE
        except ValueError:
            pass  # the date could not be parsed

        try:  # checksum
            checksum = PersonalNumberValidator.get_checksum(personal_number)
            if int(personal_number[-1]) == checksum:
                validity = validity | PersonalNumberValidity.CHECKSUM
        except ValueError:
            pass  # could not compute checksum

        return validity


class Hasher:
    """Hash method for the VAIB project, uses a sha512/256 hashing which is
    "a one way cryptographic process", i.e. irreversible.
    """

    def __init__(self, secret_salt: str) -> None:
        """Create an instance of the Hasher that has a predefined salt

        :param secret_salt: a random string that makes the hash harder to break. It gets
        prepended to the string that gets hashed.
        """
        # important because when getting the bytes anything can be used as salt.
        assert type(secret_salt) == str
        self.secret_salt = secret_salt

    def hash(self, msg: str):
        """Hash a message

        :param msg: message that we want to encrypt, normally the personnummer or the
         StudyID.
        :return: the encrypted message as hexdigest (in characters from '0' to '9' and
         'a' to 'f')
        """
        assert type(msg) == str, f"value is not of type str, {type(msg)}"
        h = SHA512.new(truncate="256")
        bytes_str = bytes(f"{self.secret_salt}{msg}", "utf-8")
        h.update(bytes_str)
        return str(h.hexdigest())

    def hash_personal_number(
        self, personal_number: str
    ) -> Tuple[str, PersonalNumberValidity]:
        """Hash the personnal number after removing all characters that are not 0-9,
        a-z, or A-Z.
        Obtain the validity of the personal number before the hashing.

        :param personal_number: personal number to be hashed
        :return: hashed string and validity of the pre-hashed pnr
        """
        pnr = re.sub(r"[^\da-zA-Z]+", "", personal_number)
        return self.hash(pnr), PersonalNumberValidator.validate(pnr)
