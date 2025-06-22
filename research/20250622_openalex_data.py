# %%
import json

import polars as pl
import start
from unidecode import unidecode

from udelar_graph.load.openalex import get_openalex_to_colibri_authors_mapping
from udelar_graph.models import Person

# %%
data = pl.read_csv("data/all-works-open-ales.csv")
data = data.with_columns(
    authors=pl.col("authorships.author.display_name").str.split("|")
).with_columns(
    authors_normalized=pl.col("authors").list.eval(
        pl.element()
        .str.to_lowercase()
        .map_elements(unidecode, return_dtype=pl.String)
        .replace(".", "")
    )
)

# %%
with open("data/colibri_people.json", "r") as f:
    colibri_people: list[Person] = [Person.model_validate(p) for p in json.load(f)]

colibri_people
# %%
oa_to_co_mapping = get_openalex_to_colibri_authors_mapping(data, colibri_people)
len(oa_to_co_mapping)
# %%
oa_to_co_mapping["gregory randall"]
# %%
# Articles with at least 2 colibri authors
data = data.filter(
    pl.col("authors_normalized")
    .list.filter(pl.element().is_in(oa_to_co_mapping.keys()))
    .list.len()
    > 1
)
data
# %%
for c in data.columns:
    print(c, data[c][0])
    print()

# %%
from udelar_graph.processing.works import normalize_work_name

openalex_works = data.select(
    pl.col("title"),
    pl.col("title")
    .map_elements(normalize_work_name, return_dtype=pl.String)
    .alias("normalized_title"),
    pl.col("abstract"),
    pl.col("authors"),
    pl.col("authors_normalized"),
    pl.col("language"),
    pl.col("type"),
    pl.col("primary_location.landing_page_url").alias("pdf_url"),
    pl.col("keywords.display_name").str.split("|").alias("keywords"),
).filter(pl.col("normalized_title").is_not_null())

openalex_works
# %%
openalex_works

# %%
from udelar_graph.models import Work

with open("data/colibri_works.json", "r") as f:
    colibri_works: list[Work] = [Work.model_validate(w) for w in json.load(f).values()]

len(colibri_works)
# %%
from udelar_graph.load.openalex import find_repeated_works

repeated_works = find_repeated_works(openalex_works, colibri_works)
len(repeated_works)
# %%
for k, v in repeated_works.items():
    openalex_data = openalex_works.filter(pl.col("normalized_title") == k).to_dicts()[0]
    colibri_data = list(filter(lambda x: x.normalized_title == v, colibri_works))[0]
    if colibri_data.abstract is None and openalex_data["abstract"] is not None:
        colibri_data.abstract = openalex_data["abstract"]
    if colibri_data.pdf_url is None and openalex_data["pdf_url"] is not None:
        colibri_data.pdf_url = openalex_data["pdf_url"]
    if colibri_data.language is None and openalex_data["language"] is not None:
        colibri_data.language = openalex_data["language"]
    if colibri_data.type is None and openalex_data["type"] is not None:
        colibri_data.type = openalex_data["type"]
# %%
new_works = [
    Work(
        normalized_title=w["normalized_title"],
        abstract=w["abstract"],
        pdf_url=w["pdf_url"],
        language=w["language"],
        type=w["type"],
    )
    for w in openalex_works.filter(
        pl.col("normalized_title").is_in(repeated_works.keys()).not_()
    ).iter_rows(named=True)
]
new_works
# %%
openalex_works = openalex_works.with_columns(
    normalized_title=pl.col("normalized_title").map_elements(
        lambda x: repeated_works.get(x, x), return_dtype=pl.String
    )
)


# %%
openalex_works.select("authors_normalized", "normalized_title").explode(
    "authors_normalized"
).with_columns(
    authors_normalized=pl.col("authors_normalized").map_elements(
        lambda x: oa_to_co_mapping[x].normalized_name
        if x in oa_to_co_mapping
        else None,
        return_dtype=pl.String,
    )
).filter(
    pl.col("authors_normalized").is_not_null(),
    pl.col("normalized_title").is_not_null(),
)
# %%
from udelar_graph.load.openalex import (
    get_person_to_work_edges,
    get_work_keywords,
    get_work_types,
)

person_to_work_edges = get_person_to_work_edges(openalex_works, oa_to_co_mapping)
person_to_work_edges
# %%
work_keywords = get_work_keywords(openalex_works)
# %%
work_types = get_work_types(openalex_works)
# %%
work_types

# %%
