import json
from pathlib import Path
from typing import Literal

import polars as pl
from loguru import logger
from unidecode import unidecode

from udelar_graph.models import Person, Work, WorkKeyword, WorkType
from udelar_graph.processing.names import get_people_list
from udelar_graph.processing.works import normalize_work_name
from udelar_graph.repository import UdelarGraphRepository


def load_colibri_data(data_dir: Path = Path("data/colibri")) -> pl.DataFrame:
    all_paths = data_dir.glob("**/*.jsonl")
    data = []
    for path in all_paths:
        if "Facultad de Ingeniería" not in str(path):
            continue
        with open(path, "r") as f:
            for line in f:
                data.append(json.loads(line))

    return pl.DataFrame(data)


def get_works(data: pl.DataFrame) -> list[Work]:
    works_df = data.select(
        pl.col("normalized_title"),
        pl.col("title"),
        pl.col("abstract"),
        pl.col("type"),
        pl.col("pdf_url"),
        pl.col("source"),
        pl.col("language"),
    )
    works: list[Work] = []
    for row in works_df.iter_rows(named=True):
        works.append(
            Work(
                normalized_title=row["normalized_title"],
                title=row["title"],
                abstract=row["abstract"],
            )
        )
    return works


def get_person_to_work_relations(
    data: pl.DataFrame,
    rel: Literal["authors", "contributors"],
    people_name_mapping: dict[str, str],
) -> list[tuple[Person, Work]]:
    return [
        (
            Person(normalized_name=row["person"], aliases=None),
            Work(normalized_title=row["normalized_title"]),
        )
        for row in (
            data.select(
                pl.col(rel).alias("person"),
                pl.col("title")
                .map_elements(normalize_work_name, return_dtype=pl.String)
                .alias("normalized_title"),
            )
            .explode("person")
            .with_columns(
                person=pl.col("person").replace_strict(
                    people_name_mapping, default=None
                )
            )
            .drop_nulls("person")
            .iter_rows(named=True)
        )
    ]


def get_work_types(data: pl.DataFrame) -> list[tuple[Work, WorkType]]:
    return [
        (Work(normalized_title=row["normalized_title"]), WorkType(type=row["type"]))
        for row in data.select(
            pl.col("normalized_title"),
            pl.col("type"),
        ).iter_rows(named=True)
    ]


def get_work_keywords(data: pl.DataFrame) -> list[tuple[Work, WorkKeyword]]:
    return [
        (
            Work(normalized_title=row["normalized_title"]),
            WorkKeyword(keyword=row["keyword"]),
        )
        for row in data.select(
            pl.col("normalized_title"),
            pl.col("keywords").list[0].str.split(";").alias("keyword"),
        )
        .explode("keyword")
        .with_columns(
            keyword=pl.col("keyword")
            .str.strip_chars()
            .map_elements(unidecode, return_dtype=pl.String)
        )
        .iter_rows(named=True)
    ]


def populate_graph_colibri(
    repository: UdelarGraphRepository, data_dir: Path = Path("data/colibri")
):
    data = load_colibri_data(data_dir)
    data = data.with_columns(
        normalized_title=pl.col("title").map_elements(
            normalize_work_name, return_dtype=pl.String
        ),
    )

    people, people_name_mapping = get_people_list(data)

    works = get_works(data)

    authorship_relations = get_person_to_work_relations(
        data, "authors", people_name_mapping
    )
    contributor_relations = get_person_to_work_relations(
        data, "contributors", people_name_mapping
    )

    work_types = get_work_types(data)
    work_keywords = get_work_keywords(data)

    logger.info(f"Creating {len(people)} people")
    repository.create_person_batch(people)
    logger.info(f"Creating {len(works)} works")
    repository.create_works_batch(works)
    logger.info(f"Creating {len(authorship_relations)} authorship relations")
    repository.create_authorship_relationship_batch(authorship_relations)
    logger.info(f"Creating {len(contributor_relations)} contributor relations")
    repository.create_contributor_relationship_batch(contributor_relations)
    logger.info(f"Creating {len(work_types)} work types")
    repository.create_work_type_batch(work_types)
    logger.info(f"Creating {len(work_keywords)} work keywords")
    repository.create_work_keyword_batch(work_keywords)
