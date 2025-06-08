from pathlib import Path

import typer

app = typer.Typer(
    name="Udelar Graph data extraction",
    help="Comandos para la extracción de datos para el proyecto.",
    no_args_is_help=True,
    invoke_without_command=False,
)


@app.command("colibri", help="Crawlear proyectos de colibri")
def crawl_colibri():
    try:
        from scrapy.crawler import CrawlerProcess

        from .colibri import ColibriSpider
    except ImportError:
        typer.secho(
            "Run `uv sync --group crawlers` to install the necessary "
            "dependencies for this operation"
        )
        exit(1)

    process = CrawlerProcess()
    process.crawl(ColibriSpider)
    process.start()
