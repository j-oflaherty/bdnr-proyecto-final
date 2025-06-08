# %%
import json
from collections import defaultdict
from pathlib import Path

import polars as pl
import start
from tqdm import tqdm

# %%
all_jsonl_files = list(Path("data/Colibri").glob("**/*.jsonl"))
all_jsonl_files[0]
# %%
data = []
for file in all_jsonl_files:
    with open(file, "r") as f:
        data.extend([json.loads(l) for l in f.readlines()])
df = pl.DataFrame(data)
# %%
df.count()
# %%
df_personas = (
    df.select(pl.concat_list("authors", "contributors").alias("people"))
    .explode("people")
    .drop_nulls()
    .unique()
)
df_personas
# %%
names = df_personas.with_columns(name_length=pl.col("people").str.len_chars()).sort(
    "name_length", descending=True
)
names
# %%
from udelar_graph.processing.names import (
    analyze_name_group,
    are_first_names_same,
    are_surnames_same,
    parse_full_name,
)

name_groups: list[set[str]] = []
already_grouped: set[str] = set()
parsed_names: dict[str, dict | None] = {}
name_list = names["people"].to_list()
for i, name in tqdm(enumerate(name_list), total=len(name_list)):
    if name in already_grouped:
        continue
    if name in parsed_names:
        name_parsed = parsed_names[name]
    else:
        name_parsed = parse_full_name(name)

    if name_parsed is None:
        continue

    name_set = {name}
    for pair in name_list[i + 1 :]:
        if pair in parsed_names:
            pair_parsed = parsed_names[pair]
        else:
            pair_parsed = parse_full_name(pair)
            parsed_names[pair] = pair_parsed

        if pair_parsed is None:
            continue

        surnames_same = are_surnames_same(
            name_parsed["surnames_parts"], pair_parsed["surnames_parts"], threshold=1
        )

        # Check first names
        first_names_same = are_first_names_same(
            name_parsed["first_names_parts"],
            pair_parsed["first_names_parts"],
            threshold=1,
        )

        if name == "De Vera, Daniel" or name == "":
            print(name, pair)
            print(are_first_names_same, are_surnames_same)

        if surnames_same and first_names_same:
            name_set.add(pair)

    name_groups.append(name_set)
    already_grouped.update(name_set)
# %%
for name_set in name_groups:
    if len(name_set) > 2:
        analysis = analyze_name_group(list(name_set))
        if not analysis["same_person"]:
            print(name_set)
            print(analysis["different_pairs"])
            print("-" * 100)
# %%
from typing import TypedDict

from unidecode import unidecode


class Person(TypedDict):
    normalized_name: str
    aliases: list[str]


people: list[Person] = []
people_to_nname_mapping = {}
for name_set in name_groups:
    shorter_name = min(name_set, key=lambda x: len(x))
    name = parse_full_name(shorter_name)
    normalized_name = f"{name['surnames_normalized']}_{name['first_names_normalized']}"
    for name in name_set:
        people_to_nname_mapping[name] = normalized_name
    people.append(Person(normalized_name=normalized_name, aliases=list(name_set)))
# %%
people
# %%
people_to_nname_mapping
# %%
people_df = pl.DataFrame(people)
people_df
# %%
