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

    # 5. Person with lowest distance to everyone
    def get_person_with_lowest_distance_query(self):
        return """
        MATCH (p:Person)
        WITH p, COUNT { (p)-[:AUTHOR_OF|CONTRIBUTOR_OF*1..]->(:Person) } AS distance
        RETURN p, distance
        ORDER BY distance ASC
        LIMIT 1
        """

    # 6. Shortest path between two people
    def get_shortest_paths_between_people_query(self, person1, person2, max_length=6):
        return f"""
        MATCH
          // Encuentra el nodo de inicio y fin usando los parámetros
          (p1:Person {{normalized_name: $person1}}),
          (p2:Person {{normalized_name: $person2}}),
          // Busca el camino más corto usando la función shortestPath
          path = shortestPath(
            // El patrón del camino, con la longitud máxima insertada como un literal
            (p1)-[:AUTHOR_OF|CONTRIBUTOR_OF*1..{max_length}]-(p2)
          )
        // Devuelve el camino completo encontrado
        RETURN path
        """

    # 7. Get all work types
    def get_possible_work_types_query(self):
        return """
        MATCH (wt:WorkType)
        RETURN wt
        """

    # 8. Get number of works by type
    def get_number_of_works_by_type_query(self):
        return """
        MATCH (w:Work)-[:TYPE]->(wt:WorkType)
        RETURN wt, COUNT(w) AS works_count
        ORDER BY works_count DESC
        """

    def close(self):
        if self.driver is not None:
            self.driver.close()


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

        # 5. Person with lowest distance
        print("🔹 Person with the lowest distance to everyone:")
        result = session.run(query.get_person_with_lowest_distance_query())
        for record in result:
            person = record["p"]
            name = person.get("aliases", [person.get("normalized_name", "Unknown")])[0]
            print(f"  Name: {name}")
            print(f"  Distance: {record['distance']}")
        print()

        # 5. Path between two people: Graciana Castro and Julian O'Flaherty
        person1 = "graciana castro"
        person2 = "lorena etcheverry"
        print(f"🔹 Shortest path between {person1} and {person2}:")
        result = session.run(
            query.get_shortest_paths_between_people_query(
                person1, person2, max_length=30
            ),
            {"person1": person1, "person2": person2},
        )
        for record in result:
            path = record["path"]
            print(f"  Path: {path}")
        print()

        # 6. Get works by a specific person
        person_name = "graciana castro"
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

        # 7. Get all people
        print("🔹 All people in the database:")
        result = session.run(query.get_all_people_query())
        for record in result:
            person = record["p"]
            print(f"{person}")

        print()

        # 8. Get all work types:
        print("🔹 Possible work types:")
        result = session.run(query.get_possible_work_types_query())
        for record in result:
            work_type = record["wt"]["type"]
            print(f"  Work Type: {work_type}")

        print()

        # 9. Get number of works by type
        print("🔹 Number of works by type:")
        result = session.run(query.get_number_of_works_by_type_query())
        for record in result:
            work_type = record["wt"]["type"]
            works_count = record["works_count"]
            print(f"  Work Type: {work_type}, Number of Works: {works_count}")
        print()

        query.close()
