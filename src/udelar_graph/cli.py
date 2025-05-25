from typer import Typer

from udelar_graph.extraction.cli import app as extraction_app

app = Typer(
    name="Udelar Graph CLI",
    help="CLI para el manejo de datos y creación de experimentos para el grafo udelar.",
    no_args_is_help=True,
)

app.add_typer(extraction_app, name="extraction")
