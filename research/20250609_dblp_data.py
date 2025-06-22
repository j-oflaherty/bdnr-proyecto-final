# %%
import numpy as np
import polars as pl
import start
from tqdm import tqdm

# %%
data = pl.read_csv("data/all-works-open-ales.csv")

# %%
data.count()
# %%
data.head()

# %%
import json

from udelar_graph.models import Person

people = [Person.model_validate(p) for p in json.load(open("data/colibri_people.json"))]

# %%
people

# %%
for c in data.columns:
    print(c)
# %%
alex_authors = (
    data.select(
        pl.col("authorships.author.display_name").alias("author").str.split("|")
    )
    .explode("author")
    .unique()
)
# %%
for row in data.select(
    pl.col("authorships.author.display_name"), pl.col("authorships.affiliations")
).iter_rows(named=True):
    print(row["authorships.author.display_name"])
    print(row["authorships.affiliations"])
    print("-" * 100)
    break
# %%
data.select(
    pl.col("authorships.author.display_name").str.split("|"),
    pl.col("authorships.affiliations").str.split("|"),
)

# %%
from unidecode import unidecode

colibri_authors = [
    {
        "name": unidecode(p.names.lower()),
        "surname": unidecode(p.surnames.lower()),
        "normalized_name": p.normalized_name,
    }
    for p in people
]

alex_authors = alex_authors.with_columns(
    decoded_name=pl.col("author")
    .str.to_lowercase()
    .map_elements(unidecode, return_dtype=pl.String)
)


# %%
def calculate_tf_overlap(name, surname, document_words):
    """Calculate term frequency overlap between query and document"""
    name_split = name.split()
    surname_split = surname.split()
    query_set = set(name_split + surname_split)
    doc_set = set(document_words)

    # Count overlapping words
    overlap = len(query_set.intersection(doc_set))
    overlap += len(doc_set.intersection([i[0] + "." for i in name_split])) / 4

    # Normalize by query length (or document length, depending on preference)
    return (
        2 * overlap / (len(name_split) + len(surname_split) + len(doc_set))
        if query_set
        else 0
    )


alex_authors_list = [n.split() for n in alex_authors["decoded_name"].unique().to_list()]

# %%
alex_to_colibri_mapping = {}
for i in tqdm(range(len(colibri_authors))):
    q_author = colibri_authors[i]
    query_name = q_author["name"].split() + q_author["surname"].split()

    # Calculate TF overlap scores for all authors
    scores = []
    for author_words in alex_authors_list:
        score = calculate_tf_overlap(
            q_author["name"], q_author["surname"], author_words
        )
        scores.append(score)

    scores_array = np.array(scores)
    max_score = scores_array.max()
    if max_score < 0.95 and (scores_array == max_score).sum() > 1:
        continue

    if (scores_array == max_score).sum() > 1:
        for i in np.nonzero(scores_array == max_score)[0]:
            alex_to_colibri_mapping[" ".join(alex_authors_list[i])] = q_author[
                "normalized_name"
            ]
        continue

    if max_score >= 0.8:
        alex_to_colibri_mapping[" ".join(alex_authors_list[scores_array.argmax()])] = (
            q_author["normalized_name"]
        )


# %%
for k, v in alex_to_colibri_mapping.items():
    print(k)
    print(v)
    print("-" * 100)
# %%
len(alex_to_colibri_mapping)

# %%
shared_authors = alex_authors.with_columns(
    colibri_name=pl.col("decoded_name").replace_strict(
        alex_to_colibri_mapping, default=None
    )
)
shared_authors = shared_authors.drop_nulls()

# %%
# Get the set of shared author names for efficient lookup
shared_author_names = set(shared_authors["author"].to_list())
shared_author_names
# %%
# Filter data to keep only rows that have at least one author from shared_authors
filtered_data = data.with_columns(
    authors=pl.col("authorships.author.display_name").str.split("|")
).filter(
    pl.col("authors").list.eval(pl.element().is_in(shared_author_names)).list.any()
)
filtered_data
# %%
print(f"Original data count: {data['id'].count()}")
print(f"Filtered data count: {filtered_data['id'].count()}")
print(f"Shared authors count: {len(shared_author_names)}")

# %%
for c in filtered_data.columns:
    print(c)
# %%
from udelar_graph.processing.works import normalize_work_name

openalex_works_df = (
    filtered_data.with_columns(keywords=pl.col("keywords.display_name").str.split("|"))
    .group_by("title", "authors", "type")
    .agg(
        pl.first("id").alias("source"),
        pl.first(
            "language",
        ),
        pl.first("abstract"),
        pl.col("best_oa_location.pdf_url"),
        pl.col("keywords").list.explode(),
    )
    .with_columns(
        pdf_url=pl.col("best_oa_location.pdf_url").list.first(),
        normalized_title=pl.col("title").map_elements(
            normalize_work_name, return_dtype=pl.String
        ),
    )
    .drop("best_oa_location.pdf_url")
    .drop_nulls("normalized_title")
)
openalex_works_df
# %%
from udelar_graph.models import Work

with open("data/colibri_works.json", "r") as f:
    works = {k: Work.model_validate(v) for k, v in json.load(f).items()}

# %%

colibri_names = [normalize_work_name(w.title) for w in works.values() if w.title]
colibri_names
# %%
openalex_names = openalex_works_df["normalized_title"].unique().drop_nulls().to_list()
# %%
for w in openalex_names:
    if "photoholmes" in w:
        print(w)
for w in colibri_names:
    if "photoholmes" in w:
        print(w)
# %%
from Levenshtein import distance
from tqdm import tqdm

colibri_works = [w for w in works.keys()]
shared_works = {}
for cw in tqdm(colibri_works):
    if cw in openalex_names:
        shared_works[cw] = cw
        continue
    if len(cw) < 20:
        continue
    for on in openalex_names:
        if distance(cw, on) < 5:
            shared_works[cw] = on
            break

# %%
new_works = openalex_works_df.with_columns(
    normalized_title=pl.col("title").map_elements(
        normalize_work_name, return_dtype=pl.String
    )
).filter(pl.col("normalized_title").is_in(list(shared_works.values())).not_())
new_works


# %%
def get_openalex_works(data: pl.DataFrame) -> list[Work]:
    works = []
    for row in data.iter_rows(named=True):
        works.append(
            Work(
                normalized_title=row["normalized_title"],
                title=row["title"],
                abstract=row["abstract"],
                pdf_url=row["pdf_url"],
                language=row["language"],
                source=row["source"],
            )
        )

    return works


# %%
new_works.select("normalized_title", "authors").explode("authors").with_columns(
    normalized_author=pl.col("authors").map_elements(
        lambda x: unidecode(x.lower()), return_dtype=pl.String
    ),
).with_columns(
    colibir_authors=pl.col("normalized_author").replace_strict(
        alex_to_colibri_mapping, default=None
    ),
).drop_nulls()

# %%
openalex_works = [
    Work.model_validate(
        dict(
            normalized_title=w["normalized_title"],
            title=w["title"],
            abstract=w["abstract"],
            pdf_url=w["pdf_url"],
            language=w["language"],
            source=w["source"],
        )
    )
    for w in new_works.iter_rows(named=True)
]
openalex_works
# %%
from pathlib import Path
from unidecode import unidecode
import json
from udelar_graph.models import Person
from udelar_graph.load.openalex import get_openalex_to_colibri_authors_mapping

data = pl.read_csv("data/all-works-open-ales.csv")
data = data.with_columns(
    authors=pl.col("authorships.author.display_name").str.split("|")
).with_columns(
    authors_normalized=pl.col("authors").list.eval(
        pl.element().str.to_lowercase().map_elements(unidecode, return_dtype=pl.String)
    )
)

# %%
with open("data/colibri_people.json", "r") as f:
# %%
# %%
from sklearn.feature_extraction.text import CountVectorizer

vectorizer = CountVectorizer(binary=True)
names = data["authors_normalized"].explode().unique().to_list()
objective_vector = vectorizer.fit_transform(names)
# %%
data.select("authors_normalized").explode("authors_normalized").unique().count()
# %%
from tqdm import tqdm
import re

openalex_to_colibri_author_mapping = {}
for i,col in tqdm(enumerate(existing_people), total=len(existing_people)):
    query = unidecode(col.names.lower() + " " + col.surnames.lower())
    exp_query = query + " " + " ".join([name.lower()[0] + "." for name in col.names.split(" ") if len(name) > 0])
    col_vector = vectorizer.transform([exp_query])
    scores = col_vector @ objective_vector.T
    max_score = scores.max()
    best_match = np.argmax(scores)
    if max_score != len(query.split(" ")):
        continue
    if abs(len(re.split(r"[- ]", names[best_match])) - len(query.split(" "))) == 2:
        continue
    openalex_to_colibri_author_mapping[query] = names[best_match]

len(openalex_to_colibri_author_mapping)
# %%
for name,k in openalex_to_colibri_author_mapping.items():
    if "fernandez" in k:
        print(name)
        print(k)
        print()
# %%
len(openalex_to_colibri_author_mapping)
# %%
import re
re.split(r"[- ]", " .-fd-sa")


# %%
mapping_path = Path("data/oa_to_co_map_old.json")
if mapping_path.exists():
    openalex_to_colibri_author_mapping_2: dict[str, str] = json.load(
        mapping_path.open("r")
    )
else:
    openalex_to_colibri_author_mapping = get_openalex_to_colibri_authors_mapping(
        data, existing_people
    )
    json.dump(
        openalex_to_colibri_author_mapping,
        mapping_path.open("w"),
    )
# %%
len(set(openalex_to_colibri_author_mapping.keys()).intersection(set(openalex_to_colibri_author_mapping_2.keys())))

# %%
existing_authors = set(openalex_to_colibri_author_mapping.keys())
data = data.filter(
    pl.col("authors_normalized").list.eval(pl.element().is_in(existing_authors)).list.any()
)
# %%
for c in data.columns:
    if "author" in c:
        print(c, data[c][0])
        print(c)
        print()

# %%
new_authors = data.select(
    pl.col("authors"),
    pl.col("authors_normalized"),
    pl.col("authorships.raw_affiliation_strings").str.split("|").list.filter(pl.element().str.len_chars() > 2).alias("institution")
)
new_authors.filter(pl.col("authors").list.len() == pl.col("institution").list.len()).explode(
    "authors", "authors_normalized", "institution"
).group_by("authors", "authors_normalized").agg(
    "institution",
).with_columns(
    pl.col("institution").list.eval(pl.element().str.to_lowercase().map_elements(unidecode, return_dtype=pl.String)).list.unique()
).filter(
    pl.col("authors").is_in(existing_authors).not_(),
    pl.col("institution").list.eval(pl.element().str.contains("republica")).list.any()
)
# %%
for author in existing_authors:
    if "luna" in author:
        print(author)
# %%
for author in existing_authors:
    if "cataldo" in author:
        print(author)
# %%

# new_authors.filter(pl.col("is_republica")).select("authors", "authors_normalized", "institution").explode("authors", "authors_normalized", "institution")
# %%
data.select(
    pl.col("authors"),
    pl.col("authorships.raw_affiliation_strings").str.split("|").list.filter(pl.element().str.len_chars() > 2),
).filter(
    pl.col("authors").list.len() != pl.col("authorships.raw_affiliation_strings").list.len()
)
# %%

print(len(data))
existing_authors = set(openalex_to_colibri_author_mapping.keys())
data = data.filter(
    pl.col("authors_normalized")
    .list.eval(pl.element().is_in(existing_authors))
    .list.any()
)
new_authors = data.select(
    "authors",
    "authors_normalized",
    pl.col("authorships.raw_affiliation_strings").str.split("|").alias("institution"),
) %%
