# %%
import json

import polars as pl
import start

# %%
with open(
    "data/Colibri/Facultad de Ingeniería/Instituto de Ingeniería Eléctrica/tesis_de_grado.jsonl",
    "r",
) as f:
    data = [json.loads(l) for l in f.readlines()]
df = pl.DataFrame(data)
df.head(10)
# %%
for col in df.columns:
    print(col)
# %%
df.select(pl.concat_list("authors", "contributors").alias("author"), "title").explode(
    "author"
).group_by("author").agg("title").with_columns(
    work_count=pl.col("title").list.len()
).sort("work_count", descending=True).head(10)
# %%
df.explode("authors").filter(pl.col("authors").str.contains(""))
# %%
df.count()
# %%
df_alex = pl.read_csv("works-2025-05-23T23-21-42.csv", infer_schema_length=10000)
df_alex.head(1)
# %%
for c in df_alex.columns:
    print(c)

# %%
df.count()

# %%
with open(
    "data/Colibri/Facultad de Ingeniería/Instituto de Ingeniería Mecánica y Producción Industrial/tesis_de_grado.jsonl",
    "r",
) as f:
    data = [json.loads(l) for l in f.readlines()]
df = pl.DataFrame(data)
df.head(1)

# %%
from pathlib import Path

all_jsonl_files = list(Path("data/Colibri").glob("**/*.jsonl"))
all_jsonl_files
# %%
data = []
for file in all_jsonl_files:
    with open(file, "r") as f:
        data.extend([json.loads(l) for l in f.readlines()])
# %%
len(data)
# %%
df = pl.DataFrame(data)
df.head(1)
# %%
df.columns
# %%
for row in (
    df.group_by("collection_path")
    .len()
    .sort("len", descending=True)
    .iter_rows(named=True)
):
    print(row["len"], "/".join(row["collection_path"]))
# %%
df.group_by("collection_path").agg(
    pl.col("authors").list.len().alias("authors_count"),
    pl.col("contributors").list.len().alias("contributors_count"),
    pl.col("title").list.len().alias("title_count"),
)
# %%
# %%
authors_df = (
    df.select(pl.concat_list("authors", "contributors").alias("people"), "title")
    .explode("people")
    .drop_nulls("people")
    .group_by("people")
    .agg(
        pl.col("title").len().alias("works_count"),
    )
    .sort("works_count", descending=True)
)
# %%
print(authors_df["people"][0])


# %%
def normalize_names(name: str) -> str | None:
    splits = name.split(",")
    if len(splits) == 2:
        first_last_name = f"{splits[1].strip()}_{splits[0].strip()}"
        return first_last_name.replace(" ", "_").lower()
    return None


# %%
for row in (
    authors_df.with_columns(
        pl.col("people")
        .map_elements(normalize_names, return_dtype=pl.String)
        .alias("name_normalized")
    )
    .filter(pl.col("name_normalized").is_null())
    .iter_rows(named=True)
):
    print(row["works_count"], row["people"])
# %% Create authors CSV
# Persona
# - Nombre
# - Apellido
# - nombre normalizado
df.count()


# %%
from unidecode import unidecode


def normalize_name(name: str) -> dict[str, str | None]:
    splits = name.split(",")
    if len(splits) == 2:
        first_name = unidecode(splits[1].strip())
        last_name = unidecode(splits[0].strip())
        normalized_name = (
            last_name.replace(" ", "_").lower().replace(".", "")
            + "_"
            + first_name.replace(" ", "_").lower().replace(".", "")
        )
        return {
            "firstName": first_name,
            "lastName": last_name,
            "normalizedName": normalized_name,
        }
    return {"firstName": None, "lastName": None, "normalizedName": None}


personas_df = (
    df.select(pl.concat_list("authors", "contributors").alias("people"))
    .explode("people")
    .drop_nulls("people")
    .unique("people")
    .with_columns(
        parsed_name=pl.col("people").map_elements(
            normalize_name,
            return_dtype=pl.Struct(
                {
                    "firstName": pl.String,
                    "lastName": pl.String,
                    "normalizedName": pl.String,
                }
            ),
        )
    )
    .unnest("parsed_name")
    .drop_nulls()
)  # .select("firstName", "lastName", "normalizedName")
# personas_df.write_csv(
#     "personas.csv",
# )
print(personas_df.count())
personas_df.head(10)
# %%
for row in (
    personas_df.group_by("normalizedName")
    .agg(pl.col("people"))
    .filter(pl.col("people").list.len() > 1)
    .iter_rows(named=True)
):
    print(row["normalizedName"], row["people"])

# %%
personas_df = (
    df.select(pl.concat_list("authors", "contributors").alias("people"))
    .explode("people")
    .drop_nulls("people")
    .unique("people")
    .with_columns(
        parsed_name=pl.col("people").map_elements(
            normalize_name,
            return_dtype=pl.Struct(
                {
                    "firstName": pl.String,
                    "lastName": pl.String,
                    "normalizedName": pl.String,
                }
            ),
        )
    )
    .unnest("parsed_name")
    .drop_nulls()
    .unique("normalizedName")
)
personas_df.count()

# %%
from collections import defaultdict

from Levenshtein import distance
from tqdm import tqdm


def is_name_subset(name1: str, name2: str) -> bool:
    # Remove accents and convert to lowercase for comparison
    name1 = unidecode(name1.lower())
    name2 = unidecode(name2.lower())

    # Split into first and last names
    last1, first1 = name1.split(",")
    last2, first2 = name2.split(",")

    # Clean up the names
    first1 = first1.strip()
    last1 = last1.strip()
    first2 = first2.strip()
    last2 = last2.strip()

    # Split into individual names
    first1_parts = first1.split()
    last1_parts = last1.split()
    first2_parts = first2.split()
    last2_parts = last2.split()

    if len(last1_parts) >= 2 and len(last2_parts) >= 2:
        if distance(last1, last2) > 3:
            return False
    if len(first1_parts) >= 2 and len(first2_parts) >= 2:
        if distance(first1, first2) > 3:
            return False

    return (
        (len(first1_parts) == 1 or len(first2_parts) == 1)
        and (len(last1_parts) == 1 or len(last2_parts) == 1)
        and first1_parts[0] == first2_parts[0]
        and last1_parts[0] == last2_parts[0]
    )


is_name_subset("Rodríguez, Juan", "Rodríguez, Juan")
# %%
names = (
    personas_df.select("people")
    .with_columns(name_length=pl.col("people").str.len_chars())
    .sort("name_length")["people"]
    .to_list()
)

grouped_names: set[str] = set()
groups: list[set[str]] = []
multiple_duplicates = []
for i, name1 in tqdm(enumerate(names), total=len(names)):
    if name1 in grouped_names:
        continue

    groups.append({name1})
    for name2 in names[i + 1 :]:
        if is_name_subset(name1, name2):
            groups[-1].add(name2)

    if len(groups[-1]) > 2:
        multiple_duplicates.append(groups[-1])
# %%
print(f"Found {len(multiple_duplicates)} duplicates")
# Print the results
for group in multiple_duplicates:
    print(group)
    print("-" * 100)
# %%
for group in groups:
    if len(group) > 2:
        for name in group:
            print(name)
        print("-" * 100)
# %%
unidecode("Rodríguez").lower()

# %%
is_name_subset("Rodríguez, Juan", "Rodríguez, Juan")


# %%
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

    # For each name in both lists, check if it can be matched
    # Each name should either match exactly/similarly or be an initial match
    for name1 in first1_parts:
        matched = False

        for name2 in first2_parts:
            # Exact or similar match for full names
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

        # If a name from first1_parts has no match, they might be different people
        # But we're lenient - only fail if it's clearly conflicting
        if not matched and len(name1) > 1:
            # Check if there's a conflicting initial
            conflicting_initial = False
            for name2 in first2_parts:
                if len(name2) == 1 and name2[0] != name1[0]:
                    # Only consider it conflicting if it's not the first name
                    # (first names are already checked above)
                    if name2 != first2_parts[0]:
                        conflicting_initial = True
                        break
            if conflicting_initial:
                return False

    # Do the same check in reverse
    for name2 in first2_parts:
        matched = False

        for name1 in first1_parts:
            # Exact or similar match for full names
            if len(name2) > 1 and len(name1) > 1:
                if distance(name2, name1) <= threshold:
                    matched = True
                    break
            # Initial matches full name
            elif len(name2) == 1 and len(name1) > 1:
                if name2[0] == name1[0]:
                    matched = True
                    break
            # Full name matches initial
            elif len(name2) > 1 and len(name1) == 1:
                if name2[0] == name1[0]:
                    matched = True
                    break
            # Both are initials
            elif len(name2) == 1 and len(name1) == 1:
                if name2[0] == name1[0]:
                    matched = True
                    break

        # If a name from first2_parts has no match, they might be different people
        # But we're lenient - only fail if it's clearly conflicting
        if not matched and len(name2) > 1:
            # Check if there's a conflicting initial
            conflicting_initial = False
            for name1 in first1_parts:
                if len(name1) == 1 and name1[0] != name2[0]:
                    # Only consider it conflicting if it's not the first name
                    # (first names are already checked above)
                    if name1 != first1_parts[0]:
                        conflicting_initial = True
                        break
            if conflicting_initial:
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


# %%
for i, group in enumerate(groups):
    if len(group) < 2:
        continue
    result = analyze_name_group(list(group))

    if not result["same_person"]:
        print("❌ DIFFERENT PEOPLE")
        print("Different pairs found:")
        for pair in result["different_pairs"]:
            print(f"  - {pair[0]} ≠ {pair[1]}")

        groups.remove(group)
        groups.extend({p} for p in group)

# %%
