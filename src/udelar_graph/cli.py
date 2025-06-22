from pathlib import Path

import typer
from typer import Typer

from udelar_graph.extraction.cli import app as extraction_app

app = Typer(
    name="Udelar Graph CLI",
    help="CLI para el manejo de datos y creación de experimentos para el grafo udelar.",
    no_args_is_help=True,
)

app.add_typer(extraction_app, name="extraction")


@app.command("colibri-load", help="Cargar datos de colibri")
def load_colibri(
    data_dir: Path = typer.Option(
        Path("data/colibri"),
        help="Directorio de datos de colibri",
    ),
    clear_db: bool = typer.Option(
        False,
        help="Borrar la base de datos antes de cargar los datos",
    ),
    extract_missing_names: bool = typer.Option(
        False,
        "--extract",
        help="Extraer nombres faltantes con openai",
    ),
):
    from neo4j import GraphDatabase

    from udelar_graph.load.colibri import populate_graph_colibri
    from udelar_graph.repository import UdelarGraphRepository

    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password"),
    )

    if clear_db:
        driver.execute_query("MATCH (n) DETACH DELETE n")

    repository = UdelarGraphRepository(driver)
    populate_graph_colibri(
        repository, data_dir=data_dir, extract_missing_names=extract_missing_names
    )
    repository.close()


@app.command("openalex-load", help="Cargar datos de openalex")
def load_openalex(
    data_dir: Path = typer.Argument(
        Path("data/all-works-open-ales.csv"),
        help="Directorio de datos de openalex",
    ),
    existing_people_json: Path = typer.Option(
        Path("data/colibri_people.json"),
        help="Archivo de personas existentes",
    ),
    existing_works_json: Path = typer.Option(
        Path("data/colibri_works.json"),
        help="Archivo de trabajos existentes",
    ),
):
    import json

    import polars as pl
    from neo4j import GraphDatabase

    from udelar_graph.load.openalex import load_openalex_works
    from udelar_graph.models import Person, Work
    from udelar_graph.repository import UdelarGraphRepository

    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password"),
    )

    repository = UdelarGraphRepository(driver)

    data = pl.read_csv(data_dir)
    colibri_people = [
        Person.model_validate(p) for p in json.load(existing_people_json.open("r"))
    ]
    colibri_works = [
        Work.model_validate(w)
        for w in json.load(existing_works_json.open("r")).values()
    ]

    load_openalex_works(
        data, repository, existing_people=colibri_people, existing_works=colibri_works
    )
