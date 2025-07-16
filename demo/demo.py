import streamlit as st
from neo4j import GraphDatabase

st.set_page_config(layout="wide")


@st.cache_resource
def get_driver():
    return GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))


@st.cache_resource
def get_all_names():
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p.names, p.surnames")
        return [record["p.names"] + " " + record["p.surnames"] for record in result]


def display_person_info(full_name, person_number):
    st.subheader(f"Person {person_number} Info")
    with driver.session() as session:
        # Get person data
        result = session.run(
            "MATCH (p:Person) WHERE p.names + ' ' + p.surnames = $name RETURN p",
            name=full_name,
        )
        person_data = result.single()
        if person_data:
            person = person_data["p"]
            st.write(f"**Normalized Name:** {person.get('normalized_name', 'N/A')}")
            st.write(f"**Names:** {person.get('names', 'N/A')}")
            st.write(f"**Surnames:** {person.get('surnames', 'N/A')}")
            if person.get("aliases"):
                st.write(f"**Aliases:** {' | '.join(person['aliases'])}")

            # Get work counts
            work_counts = session.run(
                """
                MATCH (p:Person)-[r]->(w:Work)
                WHERE p.names + ' ' + p.surnames = $name
                RETURN type(r) as relationship_type, count(w) as count
                """,
                name=full_name,
            )

            author_count = 0
            contributor_count = 0

            for record in work_counts:
                if record["relationship_type"] == "AUTHOR_OF":
                    author_count = record["count"]
                elif record["relationship_type"] == "CONTRIBUTOR_OF":
                    contributor_count = record["count"]

            st.write(f"**Works as Author:** {author_count}")
            st.write(f"**Works as Contributor:** {contributor_count}")
        else:
            st.write("Person not found in database")

    return person.get("normalized_name", "N/A")


driver = get_driver()
col1, col2 = st.columns(2)
with col1:
    st.subheader("Search Person 1")
    selected_person_1 = st.selectbox(
        "Search query for person 1",
        get_all_names(),
        key="search1",
    )

    if "selected_person_1" in locals() and selected_person_1:
        normalized_name_1 = display_person_info(selected_person_1, 1)

with col2:
    st.subheader("Search Person 2")
    selected_person_2 = st.selectbox(
        "Search query for person 2", get_all_names(), key="search2", index=1
    )

    if "selected_person_2" in locals() and selected_person_2:
        normalized_name_2 = display_person_info(selected_person_2, 2)

st.header("Shortest Path")
if "selected_person_1" in locals() and "selected_person_2" in locals():
    with driver.session() as session:
        result = session.run(
            "MATCH (p1:Person {normalized_name: $person1}), (p2:Person {normalized_name: $person2}), path = shortestPath((p1)-[:AUTHOR_OF|CONTRIBUTOR_OF*1..30]-(p2)) RETURN path",
            person1=normalized_name_1,
            person2=normalized_name_2,
        )

        path_record = result.single()

        if path_record and path_record["path"]:
            path_object = path_record["path"]

            st.subheader("🔗 Collaboration Path Found!")

            # Display path length and degrees of separation
            path_length = len(path_object.relationships)
            degrees_of_separation = path_length // 2

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Path Length", path_length)
            with col2:
                st.metric("Degrees of Separation", degrees_of_separation)
            with col3:
                st.metric("Total Steps", path_length)

            # Display the path step by step
            st.subheader("📋 Path Details")

            nodes = path_object.nodes
            relationships = path_object.relationships

            for i, (node, rel) in enumerate(zip(nodes[:-1], relationships)):
                if i % 2 == 0:  # Person node
                    person_name = node.get(
                        "aliases", [node.get("normalized_name", "Unknown")]
                    )[0]
                    st.write(f"**{i // 2 + 1}.** 👤 **{person_name}**")

                    if i + 1 < len(nodes):
                        rel_type = relationships[i].type
                        if rel_type == "AUTHOR_OF":
                            st.write("   👨‍🎓 *Author of*")
                        elif rel_type == "CONTRIBUTOR_OF":
                            st.write("    🤝 *Contributor to*")

                        work_node = nodes[i + 1]
                        work_title = work_node.get(
                            "title", work_node.get("normalized_title", "Unknown")
                        )
                        st.write(f"   📄 **{work_title}**")

                        if "year" in work_node and work_node["year"]:
                            st.write(f"   📅 Year: {work_node['year']}")

                        # Show both relationship types for the work
                        if i < len(relationships) and i + 1 < len(relationships):
                            rel_type_1 = relationships[i].type
                            rel_type_2 = relationships[i + 1].type

                            # First relationship (person to work)
                            if rel_type_1 == "AUTHOR_OF":
                                st.write("   👨‍🎓 *Author of*")
                            elif rel_type_1 == "CONTRIBUTOR_OF":
                                st.write("   🤝 *Contributor to*")

                            # # Second relationship (work to next person)
                            # if rel_type_2 == "AUTHOR_OF":
                            #     st.write("   👨‍🎓 *Author of*")
                            # elif rel_type_2 == "CONTRIBUTOR_OF":
                            #     st.write("  ⬇️ 🤝 *Contributor to*")

            # Show the final person
            if nodes:
                final_person = nodes[-1]
                final_name = final_person.get(
                    "aliases", [final_person.get("normalized_name", "Unknown")]
                )[0]
                st.write(f"**{len(nodes) // 2}.** 👤 **{final_name}**")

            # Add a visual separator
            st.divider()

            # Summary box
            with st.container():
                st.info(
                    f"🎯 **Path Summary**: {normalized_name_1} → {normalized_name_2} via {degrees_of_separation} degrees of separation"
                )

        else:
            st.error(
                f"❌ No path found between '{normalized_name_1}' and '{normalized_name_2}'"
            )
            st.write("This could mean:")
            st.write("• They have never collaborated on any work")
            st.write("• They are not connected through any collaboration network")
            st.write("• The path length exceeds the maximum limit (30 steps)")
