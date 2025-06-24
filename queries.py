# Basic query to retrieve all the nodes in the neo4j graph: use CYPHER syntax
# %%
from neo4j import GraphDatabase


# %%
class UdegraphQueries:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687", auth=("neo4j", "password")
        )

    # Basic Queries
    def get_all_nodes_query(self):
        return """
        MATCH (n)
        RETURN n
        """

    def get_all_people_query(self):
        return """
        MATCH (p:Person)
        RETURN p
        LIMIT 3
        """

    def get_all_works_query(self):
        return """
        MATCH (w:Work)
        RETURN w
        LIMIT 1
        """

    def get_person_works_query(self, person_name):
        return """
        MATCH (p:Person {normalized_name: $person_name})-[:AUTHOR_OF]->(w:Work)
        RETURN w
        """

    # Queries for specific tasks
    # 1. Person with most works
    def get_person_with_most_works_query(self):
        return """
        MATCH (p:Person)-[:AUTHOR_OF]->(w:Work)
        RETURN p, COUNT(w) AS works_count
        ORDER BY works_count DESC
        LIMIT 1
        """

    # 2. Work with most authors
    def get_works_with_most_authors_query(self):
        return """
        MATCH (w:Work)<-[:AUTHOR_OF | CONTRIBUTOR_OF]-(p:Person)
        RETURN w, COUNT(p) AS authors_count
        ORDER BY authors_count DESC
        LIMIT 1
        """

    # 3. Person with most tutored students
    def get_person_with_most_tutored_students_query(self):
        return """
        MATCH (p:Person)-[:CONTRIBUTOR_OF]->(w:Work)<-[:AUTHOR_OF]-(a:Person)
        RETURN p, COUNT(DISTINCT a) AS students_count
        ORDER BY students_count DESC
        LIMIT 1
        """

    # 4. Person with most coauthors
    def get_person_with_most_coauthors_query(self):
        return """
        MATCH (p:Person)-[:AUTHOR_OF]->(w:Work)<-[:AUTHOR_OF]-(coauthor:Person)
        WHERE p <> coauthor
        RETURN p, COUNT(DISTINCT coauthor) AS coauthors_count
        ORDER BY coauthors_count DESC
        LIMIT 1
        """

    # 5. Shortest path between two people
    def get_shortest_paths_between_people_query(self, person1, person2, max_length=30):
        return f"""
        MATCH
          (p1:Person {{normalized_name: $person1}}),
          (p2:Person {{normalized_name: $person2}}),
          path = shortestPath(
            (p1)-[:AUTHOR_OF|CONTRIBUTOR_OF*1..{max_length}]-(p2)
          )
        RETURN path
        """

    # 7. Get number of works by type
    def get_number_of_works_by_type_query(self):
        return """
        MATCH (w:Work)-[:TYPE]->(wt:WorkType)
        RETURN wt, COUNT(w) AS works_count
        ORDER BY works_count DESC
        """

    # 8. Get top 10 most used keywords that are not None
    def get_top_keywords_query(self):
        return """
        MATCH (w:Work)-[:KEYWORD]->(k:Keyword)
        WHERE k <> "None"
        RETURN k, COUNT(w) AS works_count
        ORDER BY works_count DESC
        LIMIT 20
        """

    # 9. Get top 10 duos of people who have collaborated
    def get_top_duos_query(self):
        return """
        MATCH (p1:Person)-[]->(w:Work)<-[]-(p2:Person)
        WHERE elementId(p1) < elementId(p2)
        RETURN p1.normalized_name as person1,
            p2.normalized_name as person2,
            count(w) as collaborations
        ORDER BY collaborations DESC
        LIMIT 10
        """

    # 10. Get all coauthors of a specific person
    def get_person_coauthors_query(self, person_name):
        return """
        MATCH (p:Person {normalized_name: $person_name})-[:AUTHOR_OF]->(w:Work)<-[:AUTHOR_OF]-(coauthor:Person)
        WHERE p <> coauthor
        RETURN DISTINCT coauthor
        ORDER BY coauthor.normalized_name
        """

    def close(self):
        if self.driver is not None:
            self.driver.close()


def print_collaboration_path(path):
    """
    Función auxiliar para imprimir un camino de colaboración de forma legible.
    """
    nodes = path.nodes
    relationships = path.relationships

    for i, node in enumerate(nodes):
        if "Person" in node.labels:
            print(
                f"({i}) Person: {node.get('normalized_name', 'Nombre no disponible')}"
            )
        elif "Work" in node.labels:
            print(f"({i}) Work: {node.get('normalized_title', 'Título no disponible')}")

        if i < len(relationships):
            rel = relationships[i]
            if rel.start_node == node:
                arrow = f"-[{rel.type}]->"
            else:
                arrow = f"<-[{rel.type}]-"
            print(f"     {arrow}")


if __name__ == "__main__":
    query = UdegraphQueries()

    with query.driver.session() as session:
        # 1. Person with most works
        print("🔹 Person with the most works:")
        result = session.run(query.get_person_with_most_works_query())
        for record in result:
            person = record["p"]
            name = person.get("aliases", [person.get("normalized_name", "Unknown")])[0]
            print(f"  Name: {name}")
            print(f"  Number of works: {record['works_count']}")
        print()

        # 2. Work with most authors
        print("🔹 Work with the most authors:")
        result = session.run(query.get_works_with_most_authors_query())
        for record in result:
            work = record["w"]
            title = work.get("title", work.get("normalized_title", "Unknown"))
            print(f"  Title: {title}")
            print(f"  Number of authors: {record['authors_count']}")
            if "abstract" in work:
                print(f"  Abstract: {work['abstract']}")
        print()

        # 3. Person with most tutored students
        print("🔹 Person with the most tutored students:")
        result = session.run(query.get_person_with_most_tutored_students_query())
        for record in result:
            person = record["p"]
            name = person.get("aliases", [person.get("normalized_name", "Unknown")])[0]
            print(f"  Name: {name}")
            print(f"  Number of tutored students: {record['students_count']}")
        print()

        # 4. Person with most coauthors
        print("🔹 Person with the most coauthors:")
        result = session.run(query.get_person_with_most_coauthors_query())
        for record in result:
            person = record["p"]
            name = person.get("aliases", [person.get("normalized_name", "Unknown")])[0]
            print(f"  Name: {name}")
            print(f"  Number of coauthors: {record['coauthors_count']}")
        print()

        # 5. Path between two people: Graciana Castro and Julian O'Flaherty
        person1 = "graciana castro"
        person2 = "julian o'flaherty"
        print(f"🔹 Shortest path between {person1} and {person2}:")
        result = session.run(
            query.get_shortest_paths_between_people_query(
                person1, person2, max_length=30
            ),
            {"person1": person1, "person2": person2},
        )
        path_record = result.single()

        if path_record:
            path_object = path_record["path"]
            print(f"Path length: {len(path_object.relationships)}")

            grados_separacion = len(path_object.relationships) / 2
            print(f"Separation grades: {int(grados_separacion)}")

            print_collaboration_path(path_object)

        else:
            print(f"No path was found between '{person1}' and '{person2}'.")
        print()

        # 6. Get works by a specific person
        person_name = "julian o'flaherty"
        print(f"🔹 Works by {person_name}:")
        result = session.run(
            query.get_person_works_query(person_name), {"person_name": person_name}
        )
        for record in result:
            work = record["w"]
            title = work.get("title", work.get("normalized_title", "Unknown"))
            print(f"  Title: {title}")
            if "abstract" in work:
                print(f"  Abstract: {work['abstract']}")
        print()

        # 7. Get number of works by type
        print("🔹 Number of works by type:")
        result = session.run(query.get_number_of_works_by_type_query())
        for record in result:
            work_type = record["wt"]["type"]
            works_count = record["works_count"]
            print(f"  Work Type: {work_type}, Number of Works: {works_count}")
        print()

        # 8. Get top 20 most used keywords
        print("🔹 Top 20 most used keywords:")
        result = session.run(query.get_top_keywords_query())
        for record in result:
            keyword = record["k"]["keyword"]
            works_count = record["works_count"]
            print(f"  Keyword: {keyword}, Number of Works: {works_count}")
        print()

        # 9. Get top 10 duos
        print("🔹 Top 10 duos:")
        result = session.run(query.get_top_duos_query())
        for record in result:
            person1 = record["person1"]
            person2 = record["person2"]
            collaborations = record["collaborations"]
            print(f"  Duo: {person1} & {person2}, Collaborations: {collaborations}")
        print()

        # 10. Get coauthors of a specific person
        person_name = "daniel bia"
        print(f"🔹 Coauthors of {person_name}:")
        result = session.run(
            query.get_person_coauthors_query(person_name), {"person_name": person_name}
        )
        for record in result:
            coauthor = record["coauthor"]
            name = coauthor.get(
                "aliases", [coauthor.get("normalized_name", "Unknown")]
            )[0]
            print(f"  Coauthor: {name}")

        query.close()
