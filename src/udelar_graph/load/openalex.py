import re

import numpy as np
import polars as pl
from Levenshtein import distance
from loguru import logger
from sklearn.feature_extraction.text import CountVectorizer
from tqdm import tqdm
from unidecode import unidecode

from udelar_graph.models import Person, Work, WorkKeyword, WorkType
from udelar_graph.processing.works import normalize_work_name
from udelar_graph.repository import UdelarGraphRepository


def get_openalex_to_colibri_authors_mapping(
    data: pl.DataFrame,
    existing_people: list[Person],
):
    if "authors_normalized" not in data.columns:
        raise ValueError("Missing `normalize_author` column on input dataframe.")
    openalex_authors = (
        data.explode("authors_normalized")
        .select("authors_normalized")
        .unique()
        .drop_nulls()
    )
    existing_people = list(filter(lambda x: x.names and x.surnames, existing_people))
    vectorizer = CountVectorizer(binary=True)
    colibri_texts = [
        unidecode(
            c_author.names.lower()
            + " "
            + c_author.surnames.lower()
            + " "
            + " ".join(
                [name[0] for name in c_author.names.split(" ") if len(name) > 0]
            ).lower()
        )
        .replace(".", "")
        .replace("-", " ")
        for c_author in existing_people
    ]
    colibri_names_lenghts = [
        len(c_author.names.split(" ") + c_author.surnames.split(" "))
        for c_author in existing_people
    ]
    colibri_vectors = vectorizer.fit_transform(colibri_texts)
    openalex_names = openalex_authors["authors_normalized"].unique().to_list()

    openalex_to_colibri_author_mapping = {}
    for oa_author in tqdm(
        openalex_names,
        total=len(openalex_names),
        desc="Mapping OpenAlex authors to Colibri",
    ):
        if len(oa_author.split(" ")) < 2:
            continue
        query = unidecode(oa_author.lower().replace(".", ""))
        col_vector = vectorizer.transform([query])

        scores = col_vector @ colibri_vectors.T
        max_score = scores.max()
        if max_score == 0:
            continue

        best_match = np.argmax(scores)
        if max_score != len(query.split(" ")):
            matches = np.nonzero(scores == max_score)[1]
            continue

        matches = np.nonzero(scores == max_score)[1]
        close_matches = []
        for match in matches:
            if abs(len(re.split(r"[- ]", query)) - colibri_names_lenghts[match]) > 2:
                continue
            close_matches.append(match)
        if len(close_matches) != 1:
            continue

        best_match = close_matches[0]
        openalex_to_colibri_author_mapping[oa_author] = existing_people[best_match]

    return openalex_to_colibri_author_mapping


def find_repeated_works(
    data: pl.DataFrame, existing_works: list[Work]
) -> dict[str, str]:
    if "normalized_title" not in data.columns:
        raise ValueError("Missing `normalized_title` column on dataframe")
    openalex_works = set(data["normalized_title"].unique().drop_nulls().to_list())
    repeated_works: dict[str, str] = {}
    for existing_work in tqdm(
        existing_works,
        desc="Finding repeated works",
    ):
        if existing_work.normalized_title in openalex_works:
            repeated_works[existing_work.normalized_title] = (
                existing_work.normalized_title
            )
        # title too short for meaningfull levesthain distance evaluation
        if len(existing_work.normalized_title) < 20:
            continue

        for w in openalex_works:
            if distance(existing_work.normalized_title, w) < 5:
                repeated_works[w] = existing_work.normalized_title

    return repeated_works


def get_openalex_works(
    data: pl.DataFrame, existing_works: list[Work]
) -> tuple[pl.DataFrame, list[Work], list[Work]]:
    repeated_works = find_repeated_works(data, existing_works)
    updated_works = []
    for k, v in repeated_works.items():
        updated = False
        openalex_data = data.filter(pl.col("normalized_title") == k).to_dicts()[0]
        colibri_data = list(filter(lambda x: x.normalized_title == v, existing_works))[
            0
        ]
        if colibri_data.abstract is None and openalex_data["abstract"] is not None:
            colibri_data.abstract = openalex_data["abstract"]
            updated = True
        if colibri_data.pdf_url is None and openalex_data["pdf_url"] is not None:
            colibri_data.pdf_url = openalex_data["pdf_url"]
            updated = True
        if colibri_data.language is None and openalex_data["language"] is not None:
            colibri_data.language = openalex_data["language"]
            updated = True
        if colibri_data.type is None and openalex_data["type"] is not None:
            colibri_data.type = openalex_data["type"]
            updated = True
        if updated:
            updated_works.append(colibri_data)

    new_works = [
        Work(
            normalized_title=w["normalized_title"],
            abstract=w["abstract"],
            pdf_url=w["pdf_url"],
            language=w["language"],
            type=w["type"],
        )
        for w in data.filter(
            pl.col("normalized_title").is_in(repeated_works.keys()).not_()
        ).iter_rows(named=True)
    ]

    data = data.with_columns(
        normalized_title=pl.col("normalized_title").map_elements(
            lambda x: repeated_works.get(x, x), return_dtype=pl.String
        )
    )

    return data, updated_works, new_works


def get_person_to_work_edges(
    data: pl.DataFrame,
    oa_to_existing_people_mapping: dict[str, Person],
):
    return [
        (
            Person(normalized_name=row["authors_normalized"]),
            Work(normalized_title=row["normalized_title"]),
        )
        for row in data.select(
            pl.col("authors_normalized"),
            pl.col("normalized_title"),
        )
        .explode("authors_normalized")
        .with_columns(
            authors_normalized=pl.col("authors_normalized").map_elements(
                lambda x: oa_to_existing_people_mapping[x].normalized_name
                if x in oa_to_existing_people_mapping
                else None,
                return_dtype=pl.String,
            )
        )
        .drop_nulls()
        .iter_rows(named=True)
    ]


def get_work_keywords(
    data: pl.DataFrame,
):
    return [
        (
            Work(normalized_title=row["normalized_title"]),
            WorkKeyword(keyword=row["keyword"]),
        )
        for row in data.select(
            pl.col("normalized_title"),
            pl.col("keywords")
            .list.eval(pl.element().str.to_lowercase())
            .alias("keyword"),
        )
        .explode("keyword")
        .filter(pl.col("keyword").is_not_null())
        .iter_rows(named=True)
    ]


def get_work_types(
    data: pl.DataFrame,
):
    return [
        (Work(normalized_title=row["normalized_title"]), WorkType(type=row["type"]))
        for row in data.select("normalized_title", "type").iter_rows(named=True)
    ]


def load_openalex_works(
    data: pl.DataFrame,
    repository: UdelarGraphRepository,
    *,
    existing_people: list[Person] = [],
    existing_works: list[Work] = [],
):
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
    oa_to_existing_mapping = get_openalex_to_colibri_authors_mapping(
        data, existing_people
    )

    # keep articles with at least 2 existing authors
    data = data.filter(
        pl.col("authors_normalized")
        .list.filter(pl.element().is_in(oa_to_existing_mapping.keys()))
        .list.len()
        > 1
    )

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

    openalex_works, updated_works, new_works = get_openalex_works(
        openalex_works, existing_works
    )

    author_to_work_edges = get_person_to_work_edges(
        openalex_works, oa_to_existing_mapping
    )

    work_keywords = get_work_keywords(openalex_works)
    work_types = get_work_types(openalex_works)

    logger.info(f"Updating {len(updated_works)} works")
    repository.update_works_batch(updated_works)
    logger.info(f"Creating {len(new_works)} works")
    repository.create_works_batch(new_works)
    logger.info(f"Creating {len(author_to_work_edges)} authorship relationships")
    repository.create_authorship_relationship_batch(author_to_work_edges)
    logger.info(f"Creating {len(work_keywords)} work keywords")
    repository.create_work_keyword_batch(work_keywords)
    logger.info(f"Creating {len(work_types)} work types connections")
    repository.create_work_type_batch(work_types)
