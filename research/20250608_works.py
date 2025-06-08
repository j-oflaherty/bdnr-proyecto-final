# %%
import json
from pathlib import Path

import polars as pl
import start

from udelar_graph.processing.names import find_duplicated_people

# %%
all_paths = list(Path("data/Colibri").glob("**/*.jsonl"))
all_paths
# %%
data = []
for file in all_paths:
    if "Facultad de Ingeniería" in str(file):
        with open(file, "r") as f:
            data.extend([json.loads(l) for l in f.readlines()])
df = pl.DataFrame(data)
df.count()
# %%
df_people = (
    df.select(pl.col("authors").list.concat(pl.col("contributors")).alias("people"))
    .explode("people")
    .drop_nulls()
    .unique()
    .sort(pl.col("people").str.len_chars(), descending=True)
)
df_people.head()
# %%
people_list = df_people["people"].to_list()
people, people_to_nname_mapping = find_duplicated_people(people_list)

# %%
df.head()
# %%
from udelar_graph.processing.works import normalize_work_name

workds_df = df.select(
    "title",
    pl.col("title")
    .map_elements(normalize_work_name, return_dtype=pl.String)
    .alias("normalized_title"),
    "abstract",
    "type",
    "pdf_url",
    "source",
    "language",
)
workds_df.head()
# %%
from udelar_graph.models import Work

works = []
for row in workds_df.iter_rows(named=True):
    works.append(
        Work(
            title=row["title"],
            normalized_title=row["normalized_title"],
            abstract=row["abstract"],
            type=row["type"],
            pdf_url=row["pdf_url"],
            source=row["source"],
            language=row["language"],
        )
    )
# %%
from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "password")
driver = GraphDatabase.driver(URI, auth=AUTH)


# %%
def clear_database(tx):
    query = "MATCH (n) DETACH DELETE n"
    tx.run(query)


with driver.session() as session:
    session.execute_write(clear_database)

# %% LOAD People
from tqdm import tqdm


def create_person(tx, normalized_name, aliases):
    query = "MERGE (p:Person {normalized_name: $normalized_name})"
    if aliases:
        query += " ON CREATE SET p.aliases = $aliases"
    tx.run(query, normalized_name=normalized_name, aliases=aliases)


with driver.session() as session:
    for p in tqdm(people):
        session.execute_write(create_person, p.normalized_name, p.aliases)


# %%
def create_work(tx, work: Work):
    query = """
    MERGE (w:Work {normalized_title: $normalized_title}) 
    ON CREATE SET
      w.abstract = $abstract,
      w.type = $type,
      w.pdf_url = $pdf_url,
      w.source = $source,
      w.language = $language
    ON MATCH SET
      w.abstract = $abstract,
      w.type = $type,
      w.pdf_url = $pdf_url,
      w.source = $source,
      w.language = $language
      """
    tx.run(
        query,
        normalized_title=work.normalized_title,
        abstract=work.abstract,
        type=work.type,
        pdf_url=work.pdf_url,
        source=work.source,
        language=work.language,
    )


with driver.session() as session:
    for w in tqdm(works):
        session.execute_write(create_work, w)
# %%
author_work_df = (
    df.select(
        pl.col("authors").alias("author"),
        pl.col("title")
        .map_elements(normalize_work_name, return_dtype=pl.String)
        .alias("normalized_title"),
    )
    .explode("author")
    .with_columns(
        author=pl.col("author").replace_strict(people_to_nname_mapping, default=None)
    )
).drop_nulls("author")
author_work_df.head()

# %%
contributor_work_df = (
    df.select(
        pl.col("contributors").alias("contributor"),
        pl.col("title")
        .map_elements(normalize_work_name, return_dtype=pl.String)
        .alias("normalized_title"),
    )
    .explode("contributor")
    .with_columns(
        contributor=pl.col("contributor").replace_strict(
            people_to_nname_mapping, default=None
        )
    )
).drop_nulls("contributor")
contributor_work_df.head()


# %%
def create_contributor_work_relationship(tx, contributor, normalized_title):
    query = """
    MATCH (c:Person {normalized_name: $contributor}), (w:Work {normalized_title: $normalized_title})
    MERGE (c)-[:CONTRIBUTOR_OF]->(w)
    """
    tx.run(query, contributor=contributor, normalized_title=normalized_title)


def create_author_work_relationship(tx, author, normalized_title):
    query = """
    MATCH (a:Person {normalized_name: $author}), (w:Work {normalized_title: $normalized_title})
    MERGE (a)-[:AUTHOR_OF]->(w)
    """
    tx.run(query, author=author, normalized_title=normalized_title)


with driver.session() as session:
    for row in tqdm(author_work_df.iter_rows(named=True), total=author_work_df.height):
        session.execute_write(
            create_author_work_relationship,
            row["author"],
            row["normalized_title"],
        )
    for row in tqdm(
        contributor_work_df.iter_rows(named=True), total=contributor_work_df.height
    ):
        session.execute_write(
            create_contributor_work_relationship,
            row["contributor"],
            row["normalized_title"],
        )
# %%
query = """
MATCH (p:Person)
WITH p,
     [(p)-[:AUTHOR_OF]->() | 1] as author_works,
     [(p)-[:CONTRIBUTOR_OF]->() | 1] as contributor_works
RETURN p.normalized_name as name,
       size(author_works) as author_count,
       size(contributor_works) as contributor_count,
       size(author_works) + size(contributor_works) as total_works
ORDER BY total_works DESC
LIMIT 10
"""

with driver.session() as session:
    result = session.run(query)
    for record in result:
        print(record)


# %%
work_typer_df = df.select(
    "type",
    pl.col("title")
    .map_elements(normalize_work_name, return_dtype=pl.String)
    .alias("normalized_title"),
)


def create_work_type(tx, work_normalized_title, type):
    query = """
    MATCH (w:Work {normalized_title: $work_normalized_title})
    MERGE (t:WorkType {type: $type})
    MERGE (w)-[:TYPE]->(t)
    """
    tx.run(query, work_normalized_title=work_normalized_title, type=type)


with driver.session() as session:
    for row in tqdm(work_typer_df.iter_rows(named=True), total=work_typer_df.height):
        session.execute_write(create_work_type, row["normalized_title"], row["type"])
# %%
query = """
MATCH (t:WorkType)
WITH t,
     [(t)<-[:TYPE]-() | 1] as works
RETURN t.type as type,
       size(works) as count
ORDER BY count DESC
"""

with driver.session() as session:
    result = session.run(query)
    for record in result:
        print(f"Type: {record['type']}, Count: {record['count']}")

# %%
from unidecode import unidecode

work_keywords_df = (
    df.select(
        pl.col("keywords").list[0].str.split(";").alias("keyword"),
        pl.col("title")
        .map_elements(normalize_work_name, return_dtype=pl.String)
        .alias("normalized_title"),
    )
    .explode("keyword")
    .with_columns(
        keyword=pl.col("keyword")
        .str.strip_chars()
        .map_elements(unidecode, return_dtype=pl.String)
    )
)
work_keywords_df.head()
# %%
work_keywords_df["keyword"].count(), work_keywords_df["keyword"].unique().count()


# %%
def create_work_keyword(tx, work_normalized_title, keyword):
    query = """
    MATCH (w:Work {normalized_title: $work_normalized_title})
    MERGE (k:Keyword {keyword: $keyword})
    MERGE (w)-[:KEYWORD]->(k)
    """
    tx.run(query, work_normalized_title=work_normalized_title, keyword=keyword)


with driver.session() as session:
    for row in tqdm(
        work_keywords_df.iter_rows(named=True), total=work_keywords_df.height
    ):
        session.execute_write(
            create_work_keyword, row["normalized_title"], row["keyword"]
        )

# %%
query = """
MATCH (k:Keyword)
WITH k,
     [(k)<-[:KEYWORD]-() | 1] as works
RETURN k.keyword as keyword,
       size(works) as count
ORDER BY count DESC
LIMIT 10
"""

with driver.session() as session:
    result = session.run(query)
    for record in result:
        print(f"Keyword: {record['keyword']}, Count: {record['count']}")

# %%
