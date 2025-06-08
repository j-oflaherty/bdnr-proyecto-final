import polars as pl
from Levenshtein import distance
from tqdm import tqdm
from unidecode import unidecode

from udelar_graph.models import Person


def parse_full_name(name: str) -> dict | None:
    """Parse a full name into surnames and first names"""
    parts = name.split(",")
    if len(parts) == 2 or (len(parts) == 3 and any(map(lambda x: len(x) == 0, parts))):
        surnames = parts[0].strip()
        first_names = parts[1].strip()
    elif len(parts) == 1:
        parts = name.split(" ")
        if len(parts) != 2:
            return None
        surnames = parts[0].strip()
        first_names = parts[1].strip()
    else:
        return None

    # Normalize for comparison
    surnames_normalized = unidecode(surnames.lower()).replace(" ", "_")
    first_names_normalized = unidecode(first_names.lower()).replace(" ", "_")

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

    if distance(shorter_surnames[0], longer_surnames[0]) > threshold:
        return False

    for short_surname in shorter_surnames:
        found_match = False
        for long_surname in longer_surnames:
            if distance(short_surname, long_surname) <= threshold:
                found_match = True
                break

        if not found_match:
            return False

    return True


def _check_names_match(names1: list, names2: list, threshold: int = 2) -> bool:
    """Helper function to check if names match"""
    for name1 in names1:
        matched = False

        for name2 in names2:
            # Full name matches
            if len(name1) > 1 and len(name2) > 1:
                if distance(name1, name2) <= threshold:
                    matched = True
                    break
            # Initial matches full name
            elif len(name1) == 1 and len(name2) > 1:
                if name1[0] == name2[0]:
                    matched = True
                    break
            # Full name matches initial
            elif len(name1) > 1 and len(name2) == 1:
                if name1[0] == name2[0]:
                    matched = True
                    break
            # Both are initials
            elif len(name1) == 1 and len(name2) == 1:
                if name1[0] == name2[0]:
                    matched = True
                    break

        if not matched and len(name1) > 1:
            # Check for conflicting initials
            conflicting_initial = False
            for name2 in names2:
                if (
                    len(name2) == 1
                    and name2[0] != name1[0]
                    and name2 != names2[0]  # Skip first name
                ):
                    conflicting_initial = True
                    break
            if conflicting_initial:
                return False

    return True


def are_first_names_same(
    first1_parts: list, first2_parts: list, threshold: int = 2
) -> bool:
    """
    Check if two sets of first names represent the same person
    Returns True if they're the same person, False if different
    """
    if not first1_parts or not first2_parts:
        return True

    main_name1 = first1_parts[0]
    main_name2 = first2_parts[0]

    if distance(main_name1, main_name2) > threshold:
        return False

    if len(main_name1) == 1 or len(main_name2) == 1:
        return main_name1[0] == main_name2[0]

    return _check_names_match(
        first1_parts, first2_parts, threshold
    ) and _check_names_match(first2_parts, first1_parts, threshold)


def analyze_name_group(names: list) -> dict:
    """
    Analyze a group of names to determine if they represent the same person or different
    people
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


def group_names(names: list) -> list[set[str]]:
    name_groups: list[set[str]] = []
    already_grouped: set[str] = set()
    parsed_names: dict[str, dict | None] = {}
    for i, name in tqdm(enumerate(names), total=len(names), desc="Grouping names"):
        if name in already_grouped:
            continue
        if name in parsed_names:
            name_parsed = parsed_names[name]
        else:
            name_parsed = parse_full_name(name)

        if name_parsed is None:
            continue

        name_set = {name}
        for pair in names[i + 1 :]:
            if pair in parsed_names:
                pair_parsed = parsed_names[pair]
            else:
                pair_parsed = parse_full_name(pair)
                parsed_names[pair] = pair_parsed

            if pair_parsed is None:
                continue

            surnames_same = are_surnames_same(
                name_parsed["surnames_parts"],
                pair_parsed["surnames_parts"],
                threshold=1,
            )

            # Check first names
            first_names_same = are_first_names_same(
                name_parsed["first_names_parts"],
                pair_parsed["first_names_parts"],
                threshold=1,
            )

            if surnames_same and first_names_same:
                name_set.add(pair)

        name_groups.append(name_set)
        already_grouped.update(name_set)

    return name_groups


def get_people_list(df: pl.DataFrame) -> tuple[list[Person], dict[str, str]]:
    """
    Find duplicated people in a list of names
    """
    names = (
        df.select(pl.concat_list("authors", "contributors").alias("people"))
        .explode("people")
        .drop_nulls()
        .unique()
        .sort(pl.col("people").str.len_chars(), descending=True)["people"]
        .to_list()
    )

    name_groups = group_names(names)

    people: list[Person] = []

    people_to_nname_mapping: dict[str, str] = {}
    for name_set in name_groups:
        shorter_name = min(name_set, key=lambda x: len(x))
        name_parsed = parse_full_name(shorter_name)
        if name_parsed is None:
            raise ValueError(f"Failed to parse name: {shorter_name}")

        normalized_name = f"{name_parsed['surnames_normalized']}_{name_parsed['first_names_normalized']}"
        for name in name_set:
            people_to_nname_mapping[name] = normalized_name
        people.append(Person(normalized_name=normalized_name, aliases=list(name_set)))

    return people, people_to_nname_mapping
