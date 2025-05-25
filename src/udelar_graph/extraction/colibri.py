import json
import os

import scrapy
from bs4 import BeautifulSoup
from loguru import logger
from scrapy.http.response import Response


class CollectionBasedFilePipeline:
    def __init__(self):
        self.files = {}
        self.output_dir = "data/"

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def process_item(
        self,
        item,
        spider,  # noqa: ARG002
    ):
        # Get the collection path and create a filename from it
        collection_path = item.get("collection_path", ["unknown"])
        # Use the last part of the collection path as the filename
        filename = collection_path[-1] if collection_path else "unknown"
        # Clean the filename to make it filesystem-safe
        filename = "".join(
            c for c in filename if c.isalnum() or c in (" ", "-", "_")
        ).strip()
        filename = filename.replace(" ", "_").lower()
        filename = f"{filename}.jsonl"  # Changed to jsonl extension
        filepath = os.path.join(self.output_dir, *collection_path[:-1], filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Initialize the file if it doesn't exist
        if filepath not in self.files:
            self.files[filepath] = open(filepath, "w", encoding="utf-8")

        # Write the item as a single JSON line
        self.files[filepath].write(json.dumps(item, ensure_ascii=False) + "\n")

        return item

    def close_spider(
        self,
        spider,  # noqa: ARG002
    ):
        # Close all files
        for file in self.files.values():
            file.close()


class ColibriSpider(scrapy.Spider):
    name = "ColibriSpider"

    base_url = "https://www.colibri.udelar.edu.uy"

    start_urls = ["https://www.colibri.udelar.edu.uy/jspui/handle/20.500.12008/22"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "udelar_graph.extraction.colibri.CollectionBasedFilePipeline": 300,
        }
    }

    def parse(self, response: Response):
        soup = BeautifulSoup(response.text, "html.parser")
        collections = soup.find_all("div", class_="list-group-item")
        if len(collections) > 0:
            for collection in collections:
                link = collection.find("h4").find("a")
                yield scrapy.Request(
                    f"{self.base_url}{link.get('href')}",
                )
        else:
            urls = [
                f"{self.base_url}{td.find('a').get('href')}"
                for td in soup.find_all("td", {"headers": "t2"})
            ]
            for url in urls:
                yield scrapy.Request(url, callback=self.parse_document)

            next_link = soup.find("div", align="center")

            if not next_link:
                logger.info(f"No next link found on {response.url}")
                return

            next_link = next_link.find(
                "a", string=lambda t: t and ("Siguiente" in t or "next" in t)
            )

            if next_link:
                logger.info(f"Next link: {next_link.get('href')}")
                yield scrapy.Request(
                    f"{self.base_url}{next_link.get('href')}",
                    callback=self.parse,
                )

    def parse_document(self, response: Response):
        soup = BeautifulSoup(response.text, "html.parser")

        collection_path = [
            link.text.strip()
            for link in soup.find_all("a", attrs={"name": "coleccion_cita"})
        ]

        yield {
            "title": response.xpath('//meta[@name="DC.title"]/@content').get(),
            "authors": response.xpath('//meta[@name="DC.creator"]/@content').getall(),
            "contributors": [  # Student appear with "Universidad" in the name
                contributor.strip()
                for contributor in response.xpath(
                    '//meta[@name="DC.contributor"]/@content'
                ).getall()
                if "Universidad" not in contributor
            ],
            "abstract": response.xpath(
                '//meta[@name="DCTERMS.abstract"]/@content'
            ).get(),
            "date": response.xpath('//meta[@name="citation_date"]/@content').get(),
            "publisher": response.xpath(
                '//meta[@name="citation_publisher"]/@content'
            ).get(),
            "subjects": response.xpath('//meta[@name="DC.subject"]/@content').getall(),
            "type": response.xpath('//meta[@name="DC.type"]/@content').get(),
            "language": response.xpath('//meta[@name="DC.language"]/@content').get(),
            "extent": response.xpath('//meta[@name="DCTERMS.extent"]/@content').get(),
            "pdf_url": response.xpath(
                '//meta[@name="citation_pdf_url"]/@content'
            ).get(),
            "keywords": response.xpath(
                '//meta[@name="citation_keywords"]/@content'
            ).getall(),
            "collection_path": collection_path,
            "source": response.url,
        }
