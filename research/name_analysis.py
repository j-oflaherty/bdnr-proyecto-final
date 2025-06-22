from collections import defaultdict

from Levenshtein import distance
from unidecode import unidecode


def parse_full_name(name: str) -> dict | None:
    """Parse a full name into surnames and first names"""
    parts = name.split(",")
    if len(parts) != 2:
        return None

    surnames = parts[0].strip()
    first_names = parts[1].strip()

    # Normalize for comparison
    surnames_normalized = unidecode(surnames.lower())
    first_names_normalized = unidecode(first_names.lower())

    return {
        "surnames": surnames,
        "first_names": first_names,
        "surnames_normalized": surnames_normalized,
        "first_names_normalized": first_names_normalized,
        "surnames_parts": surnames_normalized.split(),
        "first_names_parts": first_names_normalized.split(),
    }


def are_surnames_same(
    surnames1_parts: list, surnames2_parts: list, threshold: int = 2
) -> bool:
    """
    Check if two sets of surnames represent the same person
    Returns True if they're the same person, False if different
    """
    # If one person has more surnames than the other, check if the shorter one is contained
    shorter_surnames = (
        surnames1_parts
        if len(surnames1_parts) <= len(surnames2_parts)
        else surnames2_parts
    )
    longer_surnames = (
        surnames2_parts
        if len(surnames1_parts) <= len(surnames2_parts)
        else surnames1_parts
    )

    # Check if all surnames in the shorter list match (with misspelling tolerance) surnames in the longer list
    for short_surname in shorter_surnames:
        found_match = False
        for long_surname in longer_surnames:
            if distance(short_surname, long_surname) <= threshold:
                found_match = True
                break
        if not found_match:
            return False

    return True


def are_first_names_same(
    first1_parts: list, first2_parts: list, threshold: int = 2
) -> bool:
    """
    Check if two sets of first names represent the same person
    Returns True if they're the same person, False if different
    """
    # If one is empty, they could be the same person (omitted names)
    if not first1_parts or not first2_parts:
        return True

    # Check if the first name (most important) is the same or similar
    main_name1 = first1_parts[0]
    main_name2 = first2_parts[0]

    # If main first names are very different, they're different people
    if distance(main_name1, main_name2) > threshold:
        return False

    # Check for initials - if one has just an initial, it could match
    if len(main_name1) == 1 or len(main_name2) == 1:
        return main_name1[0] == main_name2[0]

    # If main names are similar enough, check if additional names conflict
    shorter_names = (
        first1_parts if len(first1_parts) <= len(first2_parts) else first2_parts
    )
    longer_names = (
        first2_parts if len(first1_parts) <= len(first2_parts) else first1_parts
    )

    # For each name in the shorter list, it should match something in the longer list
    for i, short_name in enumerate(shorter_names):
        if i < len(longer_names):
            # Check if they're similar or if one is an initial
            if len(short_name) == 1 or len(longer_names[i]) == 1:
                if short_name[0] != longer_names[i][0]:
                    return False
            elif distance(short_name, longer_names[i]) > threshold:
                return False

    return True


def analyze_name_group(names: list) -> dict:
    """
    Analyze a group of names to determine if they represent the same person or different people
    """
    parsed_names = []
    for name in names:
        parsed = parse_full_name(name)
        if parsed:
            parsed_names.append(parsed)

    if len(parsed_names) < 2:
        return {"same_person": True, "analysis": "Single name or unparseable names"}

    # Compare each pair of names
    different_people = []
    analysis_details = []

    for i in range(len(parsed_names)):
        for j in range(i + 1, len(parsed_names)):
            name1 = parsed_names[i]
            name2 = parsed_names[j]

            # Check surnames
            surnames_same = are_surnames_same(
                name1["surnames_parts"], name2["surnames_parts"]
            )

            # Check first names
            first_names_same = are_first_names_same(
                name1["first_names_parts"], name2["first_names_parts"]
            )

            analysis_details.append(
                {
                    "name1": name1["surnames"] + ", " + name1["first_names"],
                    "name2": name2["surnames"] + ", " + name2["first_names"],
                    "surnames_same": surnames_same,
                    "first_names_same": first_names_same,
                    "same_person": surnames_same and first_names_same,
                }
            )

            if not (surnames_same and first_names_same):
                different_people.append((names[i], names[j]))

    # If any pair is identified as different people, the group contains different people
    same_person = len(different_people) == 0

    return {
        "same_person": same_person,
        "different_pairs": different_people,
        "analysis_details": analysis_details,
    }
