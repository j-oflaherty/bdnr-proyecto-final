from unidecode import unidecode


def normalize_work_name(title: str) -> str:
    """Normalize a work title by unidecoding and replacing spaces with underscores"""
    return unidecode(title.lower()).replace(" ", "_")
