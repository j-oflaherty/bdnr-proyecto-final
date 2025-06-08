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
    populate_graph_colibri(repository, data_dir=data_dir)
    repository.close()
