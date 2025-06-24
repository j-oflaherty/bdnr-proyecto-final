import asyncio
import json
from pathlib import Path
from typing import Literal

import polars as pl
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_async
from unidecode import unidecode

from udelar_graph.models import Person, Work, WorkKeyword, WorkType
from udelar_graph.processing.names import (
    StructuredNameResponse,
    extract_person_name,
    get_people_list,
)
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
            Person(normalized_name=row["person"]),
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


def get_work_keywords(
    data: pl.DataFrame, excluded_keyworks: set[str] = set()
) -> list[tuple[Work, WorkKeyword]]:
    keywords_df = (
        data.select(
            pl.col("normalized_title"),
            pl.col("keywords").list[0].str.split(";").alias("keyword"),
        )
        .explode("keyword")
        .with_columns(
            keyword=pl.col("keyword")
            .str.strip_chars()
            .replace(".", "")
            .map_elements(unidecode, return_dtype=pl.String)
            .str.to_lowercase()
        )
    )
    if excluded_keyworks:
        keywords_df = keywords_df.filter(
            pl.col("keyword").is_in(excluded_keyworks).not_()
        )
    return [
        (
            Work(normalized_title=row["normalized_title"]),
            WorkKeyword(keyword=row["keyword"]),
        )
        for row in keywords_df.iter_rows(named=True)
    ]


def populate_graph_colibri(
    repository: UdelarGraphRepository,
    data_dir: Path = Path("data/colibri"),
    *,
    extract_missing_names: bool = False,
):
    data = load_colibri_data(data_dir)
    data = data.with_columns(
        normalized_title=pl.col("title").map_elements(
            normalize_work_name, return_dtype=pl.String
        ),
    )

    people, people_name_mapping = get_people_list(data)
    with open("data/extracted_names.json", "r") as f:
        extracted_names: dict[str, StructuredNameResponse] = {
            k: StructuredNameResponse.model_validate(v) for k, v in json.load(f).items()
        }

    missing_people = []
    filter_people = set()
    for person in tqdm(people):
        extracted_name = extracted_names.get(person.normalized_name)
        if extracted_name is None:
            missing_people.append(person)
            continue

        if not extracted_name.person:
            filter_people.add(person.normalized_name)
            continue

        person.names = extracted_name.first_names
        person.surnames = extracted_name.surnames

    if len(missing_people) > 0:
        logger.info(f"{len(missing_people)} missing names")
    if extract_missing_names:
        logger.info("Extracting with openai")
        async_pbar = tqdm_async(total=len(missing_people), desc="Extracting names")
        tasks = [
            extract_person_name(person, pbar=async_pbar) for person in missing_people
        ]
        event_loop = asyncio.get_event_loop()
        extracted_missing_names = event_loop.run_until_complete(asyncio.gather(*tasks))
        for person, extracted_name in zip(missing_people, extracted_missing_names):
            if extracted_name is not None:
                person.names = extracted_name.first_names
                person.surnames = extracted_name.surnames
                extracted_names[person.normalized_name] = extracted_name
            else:
                logger.warning(f"Failed to extract name for {person.normalized_name}")
        async_pbar.close()

        with open("data/extracted_names.json", "w") as f:
            logger.info("Saving extracted names")
            json.dump(
                {k: v.model_dump(mode="json") for k, v in extracted_names.items()},
                f,
                indent=4,
            )

    logger.info(f"Filtering {len(filter_people)} people")
    if len(filter_people) > 0:
        logger.info(f"Filtering {len(filter_people)} people")
        people = list(filter(lambda p: p.normalized_name not in filter_people, people))

    # New normalized names and join people with the same name
    logger.info("Joining people with the same extracted name")
    final_people_list: list[Person] = []
    index = 0
    new_normalized_names_mapping: dict[str, int] = {}
    reversed_people_name_mapping: dict[str, str] = {
        v: k for k, v in people_name_mapping.items()
    }
    for person in people:
        if person.names is None or person.surnames is None:
            continue

        old_normalized_name = person.normalized_name

        person.normalized_name = (
            unidecode(person.names.lower() + " " + person.surnames.lower())
            .replace(".", "")
            .replace("_", " ")
        )

        people_name_mapping[reversed_people_name_mapping[old_normalized_name]] = (
            person.normalized_name
        )

        if person.normalized_name not in new_normalized_names_mapping:
            new_normalized_names_mapping[person.normalized_name] = index
            final_people_list.append(person)
            index += 1
        else:
            final_people_list[
                new_normalized_names_mapping[person.normalized_name]
            ].aliases.extend(person.aliases)

    people = final_people_list
    logger.info(f"Final number of people: {len(people)}")

    with open("data/colibri_people.json", "w") as f:
        json.dump([p.model_dump(mode="json") for p in people], f, indent=4)

    works = get_works(data)
    with open("data/colibri_works.json", "w") as f:
        json.dump(
            {w.normalized_title: w.model_dump(mode="json") for w in works},
            f,
            indent=4,
        )

    authorship_relations = get_person_to_work_relations(
        data, "authors", people_name_mapping
    )
    contributor_relations = get_person_to_work_relations(
        data, "contributors", people_name_mapping
    )

    work_types = get_work_types(data)
    work_types_string = set({wt[1].type for wt in work_types})
    work_keywords = get_work_keywords(data, work_types_string)

    logger.info(f"Creating {len(people)} people")
    repository.create_person_batch(people)
    logger.info(f"Creating {len(works)} works")
    repository.create_works_batch(works)
    logger.info(f"Creating {len(authorship_relations)} authorship relations")
    repository.create_authorship_relationship_batch(authorship_relations)
    logger.info(f"Creating {len(contributor_relations)} contributor relations")
    repository.create_contributor_relationship_batch(contributor_relations)
    logger.info(
        f"Creating {len(work_types_string)} work types and {len(work_types)} "
        "WorkType relations"
    )
    repository.create_work_type_batch(work_types)
    logger.info(
        f"Creating {len(set(wk[1].keyword for wk in work_keywords))} work keywords and "
        f"{len(work_keywords)} WorkKeyword relations"
    )
    repository.create_work_keyword_batch(work_keywords)
