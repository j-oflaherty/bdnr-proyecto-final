import re

from unidecode import unidecode


def normalize_work_name(title: str) -> str:
    """Normalize a work title by unidecoding and replacing spaces with underscores"""
    no_punctuation = re.sub(r"[^\w\s]", "", title)
    no_spaces = re.sub(r"\s+", "_", no_punctuation)
    return unidecode(no_spaces.lower()).replace("-", "")
