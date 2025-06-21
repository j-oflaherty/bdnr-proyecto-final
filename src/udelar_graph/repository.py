from dataclasses import dataclass
from typing import Literal

from neo4j import Driver as Neo4jDriver
from neo4j import ManagedTransaction

from udelar_graph.models import Person, Work, WorkKeyword, WorkType


@dataclass
class UdelarGraphRepository:
    """Repository class for managing Udelar graph data in Neo4j."""

    driver: Neo4jDriver

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def _create_person_tx(self, tx: ManagedTransaction, person: Person):
        """Create or update a person node in the transaction.

        Args:
            tx: Neo4j transaction
            person: Person object to create/update
        """
        query = """\
        MERGE (p:Person {normalized_name: $normalized_name})
        ON CREATE SET p.aliases = $aliases
        ON MATCH SET p.aliases = $aliases
        """
        tx.run(query, normalized_name=person.normalized_name, aliases=person.aliases)

    def _create_person_batch_tx(self, tx: ManagedTransaction, persons: list[Person]):
        """Create or update multiple person nodes in the transaction.

        Args:
            tx: Neo4j transaction
            persons: List of Person objects to create/update
        """
        for p in persons:
            self._create_person_tx(tx, p)

    def create_person(self, person: Person):
        """Create or update a single person node.

        Args:
            person: Person object to create/update
        """
        with self.driver.session() as session:
            session.execute_write(self._create_person_tx, person)

    def create_person_batch(self, persons: list[Person]):
        """Create or update multiple person nodes.

        Args:
            persons: List of Person objects to create/update
        """
        with self.driver.session() as session:
            session.execute_write(self._create_person_batch_tx, persons)

    def _create_work_tx(self, tx: ManagedTransaction, work: Work):
        """Create or update a work node in the transaction.

        Args:
            tx: Neo4j transaction
            work: Work object to create/update
        """
        query = """
        MERGE (w:Work {normalized_title: $normalized_title}) 
        ON CREATE SET w.title = $title,
                      w.abstract = $abstract,
                      w.type = $type,
                      w.pdf_url = $pdf_url,
                      w.source = $source,
                      w.language = $language
        ON MATCH SET w.title = $title,
                      w.abstract = $abstract,
                      w.type = $type,
                      w.pdf_url = $pdf_url,
                      w.source = $source,
                      w.language = $language
        """
        tx.run(
            query,
            normalized_title=work.normalized_title,
            title=work.title,
            abstract=work.abstract,
            type=work.type,
            pdf_url=work.pdf_url,
            source=work.source,
            language=work.language,
        )

    def _create_work_batch_tx(self, tx: ManagedTransaction, works: list[Work]):
        """Create or update multiple work nodes in the transaction.

        Args:
            tx: Neo4j transaction
            works: List of Work objects to create/update
        """
        for w in works:
            self._create_work_tx(tx, w)

    def create_work(self, work: Work):
        """Create or update a single work node.

        Args:
            work: Work object to create/update
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_tx, work)

    def create_works_batch(self, works: list[Work]):
        """Create or update multiple work nodes.

        Args:
            works: List of Work objects to create/update
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_batch_tx, works)

    def _create_work_type_tx(self, tx: ManagedTransaction, work: Work, type: WorkType):
        """Create a work type relationship in the transaction.

        Args:
            tx: Neo4j transaction
            work: Work object
            type: WorkType object
        """
        query = """
        MATCH (w:Work {normalized_title: $normalized_title})
        MERGE (t:WorkType {type: $type})
        MERGE (w)-[:TYPE]->(t)
        """
        tx.run(query, normalized_title=work.normalized_title, type=type.type)

    def _create_work_type_batch_tx(
        self, tx: ManagedTransaction, rels: list[tuple[Work, WorkType]]
    ):
        """Create multiple work type relationships in the transaction.

        Args:
            tx: Neo4j transaction
            rels: List of (Work, WorkType) tuples to create relationships for
        """
        for w, t in rels:
            self._create_work_type_tx(tx, w, t)

    def create_work_type(self, work: Work, type: WorkType):
        """Create a single work type relationship.

        Args:
            work: Work object
            type: WorkType object
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_type_tx, work, type)

    def create_work_type_batch(
        self,
        rels: list[tuple[Work, WorkType]],
    ):
        """Create multiple work type relationships.

        Args:
            rels: List of (Work, WorkType) tuples to create relationships for
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_type_batch_tx, rels)

    def _create_work_keyword_tx(
        self, tx: ManagedTransaction, work: Work, keyword: WorkKeyword
    ):
        """Create a work keyword relationship in the transaction.

        Args:
            tx: Neo4j transaction
            work: Work object
            keyword: WorkKeyword object
        """
        query = """
        MATCH (w:Work {normalized_title: $normalized_title})
        MERGE (k:Keyword {keyword: $keyword})
        MERGE (w)-[:KEYWORD]->(k)
        """
        tx.run(query, normalized_title=work.normalized_title, keyword=keyword.keyword)

    def _create_work_keyword_batch_tx(
        self, tx: ManagedTransaction, rels: list[tuple[Work, WorkKeyword]]
    ):
        """Create multiple work keyword relationships in the transaction.

        Args:
            tx: Neo4j transaction
            rels: List of (Work, WorkKeyword) tuples to create relationships for
        """
        for w, k in rels:
            self._create_work_keyword_tx(tx, w, k)

    def create_work_keyword(self, work: Work, keyword: str):
        """Create a single work keyword relationship.

        Args:
            work: Work object
            keyword: Keyword string
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_keyword_tx, work, keyword)

    def create_work_keyword_batch(self, rels: list[tuple[Work, WorkKeyword]]):
        """Create multiple work keyword relationships.

        Args:
            rels: List of (Work, WorkKeyword) tuples to create relationships for
        """
        with self.driver.session() as session:
            session.execute_write(self._create_work_keyword_batch_tx, rels)

    def _create_people_to_work_tx(
        self,
        tx: ManagedTransaction,
        person: Person,
        work: Work,
        rel: Literal["AUTHOR_OF", "CONTRIBUTOR_OF"],
    ):
        """Create a person-work relationship in the transaction.

        Args:
            tx: Neo4j transaction
            person: Person object
            work: Work object
            rel: Relationship type ("AUTHOR_OF" or "CONTRIBUTOR_OF")
        """
        query = f"""\
        MATCH
            (p:Person {{normalized_name: $normalized_name}}),
            (w:Work {{normalized_title: $normalized_title}})
        MERGE (p)-[:{rel}]->(w)
        """
        tx.run(
            query,
            normalized_name=person.normalized_name,
            normalized_title=work.normalized_title,
            rel=rel,
        )

    def _create_people_to_work_batch_tx(
        self,
        tx: ManagedTransaction,
        rels: list[tuple[Person, Work]],
        rel: Literal["AUTHOR_OF", "CONTRIBUTOR_OF"],
    ):
        """Create multiple person-work relationships in the transaction.

        Args:
            tx: Neo4j transaction
            rels: List of (Person, Work) tuples to create relationships for
            rel: Relationship type ("AUTHOR_OF" or "CONTRIBUTOR_OF")
        """
        for p, w in rels:
            self._create_people_to_work_tx(tx, p, w, rel)

    def create_authorship_relationship(self, person: Person, work: Work):
        """Create a single authorship relationship.

        Args:
            person: Person object
            work: Work object
        """
        with self.driver.session() as session:
            session.execute_write(
                self._create_people_to_work_tx, person, work, "AUTHOR_OF"
            )

    def create_contributor_relationship(self, person: Person, work: Work):
        with self.driver.session() as session:
            session.execute_write(
                self._create_people_to_work_tx, person, work, "CONTRIBUTOR_OF"
            )

    def create_authorship_relationship_batch(self, rels: list[tuple[Person, Work]]):
        with self.driver.session() as session:
            session.execute_write(
                self._create_people_to_work_batch_tx, rels, "AUTHOR_OF"
            )

    def create_contributor_relationship_batch(self, rels: list[tuple[Person, Work]]):
        with self.driver.session() as session:
            session.execute_write(
                self._create_people_to_work_batch_tx, rels, "CONTRIBUTOR_OF"
            )
