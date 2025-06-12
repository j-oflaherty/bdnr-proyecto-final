# Basic query to retrieve all the nodes in the neo4j graph: use CYPHER syntax
from neo4j import GraphDatabase


class UdegraphQueries:
    def __init__(self):
        # init connection to the Neo4j database
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687", auth=("neo4j", "password")
        )

    def get_all_nodes_query(self):
        return """
        MATCH (n)
        RETURN n
        """

    def get_all_people_query(self):
        return """
        MATCH (p:Person)
        RETURN p
        """

    def get_all_works_query(self):
        return """
        MATCH (w:Work)
        RETURN w
        LIMIT 1
        """

    def get_person_with_most_works_query(self):
        return """
        MATCH (p:Person)-[:AUTHOR_OF]->(w:Work)
        RETURN p, COUNT(w) AS works_count
        ORDER BY works_count DESC
        LIMIT 1
        """

    def get_works_with_most_authors_query(self):
        return """
        MATCH (w:Work)<-[:AUTHOR_OF]-(p:Person)
        RETURN w, COUNT(p) AS authors_count
        ORDER BY authors_count DESC
        LIMIT 1
        """

    def get_person_with_most_tutored_students_query(self):
        return """
        MATCH (p:Person)-[:CONTRIBUTOR_OF]->(w:Work)<-[:AUTHOR_OF]-(a:Person)
        RETURN p, COUNT(DISTINCT a) AS students_count
        ORDER BY students_count DESC
        LIMIT 1
        """

    # Person with most coauthors in all its works
    def get_person_with_most_coauthors_query(self):
        return """
        MATCH (p:Person)-[:AUTHOR_OF]->(w:Work)<-[:AUTHOR_OF]-(coauthor:Person)
        WHERE p <> coauthor
        RETURN p, COUNT(DISTINCT coauthor) AS coauthors_count
        ORDER BY coauthors_count DESC
        LIMIT 1
        """

    # Person with lowest distance to every other person
    def get_person_with_lowest_distance_query(self):
        return """
        MATCH (p:Person)
        WITH p, COUNT { (p)-[:AUTHOR_OF|CONTRIBUTOR_OF*1..]->(:Person) } AS distance
        RETURN p, distance
        ORDER BY distance ASC
        LIMIT 1
        """

    def close(self):
        if self.driver is not None:
            self.driver.close()


# main to run query
if __name__ == "__main__":
    query = UdegraphQueries()

    with query.driver.session() as session:
        result = session.run(query.get_person_with_lowest_distance_query())
        for record in result:
            print(record)

    query.close()
