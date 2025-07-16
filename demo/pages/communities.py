import pandas as pd
import streamlit as st
from neo4j import GraphDatabase, exceptions

st.set_page_config(page_title="Community Analysis", layout="wide")


@st.cache_resource
def get_driver():
    return GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))


@st.cache_data
def get_all_person_names():
    """Get all person names for search functionality"""
    with driver.session() as session:
        result = session.run(
            "MATCH (p:Person) RETURN p.normalized_name as name ORDER BY name"
        )
        return [record["name"] for record in result]


@st.cache_data
def create_coauthor_graph():
    """Create or verify the coauthor graph projection exists"""
    with driver.session() as session:
        # Check if graph exists
        graph_exists = False
        try:
            # This is the modern way for GDS > 2.0
            exists_result = session.run(
                "CALL gds.graph.exists('coauthor-graph')"
            ).single()
            if exists_result:
                graph_exists = exists_result["exists"]
        except exceptions.ClientError as e:
            # Fallback for older GDS versions
            if "There is no procedure with the name `gds.graph.exists`" in str(e):
                graphs_df = session.run("CALL gds.graph.list() YIELD graphName").to_df()
                if "coauthor-graph" in graphs_df["graphName"].tolist():
                    graph_exists = True
            else:
                raise
        if not graph_exists:
            # Create the graph projection
            create_query = """
            MATCH (p1:Person)-[:AUTHOR_OF|CONTRIBUTOR_OF]->(w:Work)<-[:AUTHOR_OF|CONTRIBUTOR_OF]-(p2:Person)
            WHERE p1 < p2
            WITH p1 as source, p2 as target, count(w) as weight
            WITH gds.graph.project(
                'coauthor-graph',
                source,
                target,
                {relationshipProperties: {weight: weight}},
                {undirectedRelationshipTypes: ['*']}
            ) as g
            RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """
            result = session.run(create_query).single()
            return dict(result) if result else {}
        else:
            # Get existing graph stats
            stats_query = """
            CALL gds.graph.list('coauthor-graph')
            YIELD graphName, nodeCount, relationshipCount
            RETURN graphName as graph, nodeCount as nodes, relationshipCount as rels
            """
            result = session.run(stats_query).single()
            return dict(result) if result else {}


@st.cache_data
def get_louvain_communities():
    """Get communities using Louvain algorithm"""
    with driver.session() as session:
        # Get node degrees for ranking within communities
        degree_query = """
        CALL gds.degree.stream('coauthor-graph', {})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).normalized_name AS name, score
        """
        popularity_df = session.run(degree_query).to_df()

        # Get Louvain communities
        louvain_query = """
        CALL gds.louvain.stream('coauthor-graph', {})
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).normalized_name AS name, communityId
        """
        communities_df = session.run(louvain_query).to_df()

        # Merge with popularity scores
        result_df = communities_df.merge(popularity_df, on="name", how="left")

        # Calculate community totals and sort
        community_totals = (
            result_df.groupby("communityId")["score"].sum().sort_values(ascending=False)
        )

        return result_df, community_totals


@st.cache_data
def get_label_propagation_communities():
    """Get communities using Label Propagation algorithm"""
    with driver.session() as session:
        # Get node degrees for ranking within communities
        degree_query = """
        CALL gds.degree.stream('coauthor-graph', {})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).normalized_name AS name, score
        """
        popularity_df = session.run(degree_query).to_df()

        # Get Label Propagation communities
        lpa_query = """
        CALL gds.labelPropagation.stream('coauthor-graph', {})
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).normalized_name AS name, communityId
        """
        communities_df = session.run(lpa_query).to_df()

        # Merge with popularity scores
        result_df = communities_df.merge(popularity_df, on="name", how="left")

        # Calculate community totals and sort
        community_totals = (
            result_df.groupby("communityId")["score"].sum().sort_values(ascending=False)
        )

        return result_df, community_totals


def search_person_community(person_name, communities_df):
    """Find which community a specific person belongs to"""
    person_row = communities_df[communities_df["name"] == person_name]
    if not person_row.empty:
        return person_row.iloc[0]["communityId"]
    return None


def display_community_members(community_df, community_id, max_display=15):
    """Display members of a specific community"""
    community_members = community_df[
        community_df["communityId"] == community_id
    ].sort_values("score", ascending=False)

    total_members = len(community_members)
    display_members = community_members.head(max_display)

    cols = st.columns(3)
    for i, member in enumerate(display_members.to_dict(orient="records")):
        col_idx = i % 3
        with cols[col_idx]:
            st.write(f"👤 **{member['name']}**")
            st.write(f"   📊 Score: {member['score']:.1f}")

    if total_members > max_display:
        with st.expander(f"Show all {total_members - max_display} remaining members"):
            remaining_members = community_members.tail(total_members - max_display)
            remaining_cols = st.columns(3)
            for i, member in enumerate(remaining_members.to_dict(orient="records")):
                col_idx = i % 3
                with remaining_cols[col_idx]:
                    st.write(f"👤 **{member['name']}**")
                    st.write(f"   📊 Score: {member['score']:.1f}")


# Initialize driver
driver = get_driver()

# Title and description
st.title("🔗 Community Analysis Dashboard")
st.markdown("""
This dashboard showcases community detection in the UdelaR academic collaboration network. 
Communities represent groups of researchers who frequently collaborate on academic works.
""")

# Create coauthor graph if it doesn't exist
with st.spinner("Initializing coauthor graph..."):
    graph_info = create_coauthor_graph()

st.success(
    f"📊 Graph initialized: {graph_info['nodes']} researchers, {graph_info['rels']} collaborations"
)

# Main page search
st.header("🔍 Find a Researcher's Community")
person_names = get_all_person_names()
selected_person = st.selectbox(
    "Search for a person to see their community. Leave unselected to explore all communities.",
    ["Select a person..."] + person_names,
    key="person_search",
)


# Main content
if selected_person == "Select a person...":
    st.header("Community Exploration")
    st.markdown("Browse through the top communities detected by different algorithms.")
    tab1, tab2 = st.tabs(["🏛️ Louvain Communities", "🏷️ Label Propagation Communities"])

    with tab1:
        st.subheader("Louvain Algorithm Communities")
        st.markdown(
            """
        The Louvain algorithm optimizes **modularity** to find high-quality communities. 
        It identifies groups with dense internal connections and sparse external connections.
        """
        )

        with st.spinner("Running Louvain community detection..."):
            louvain_df, louvain_totals = get_louvain_communities()

        # Display communities
        st.write(f"📈 Found {len(louvain_totals)} communities")

        # Show top communities
        top_communities = louvain_totals.head(10)

        for i, (community_id, total_score) in enumerate(top_communities.items()):
            community_members = louvain_df[louvain_df["communityId"] == community_id]
            member_count = len(community_members)

            with st.expander(
                f"🏛️ Community {i + 1} (ID: {community_id}) - {member_count} members (Total Score: {total_score:.1f})"
            ):
                display_community_members(louvain_df, community_id)

    with tab2:
        st.subheader("Label Propagation Algorithm Communities")
        st.markdown(
            """
        Label Propagation is a fast heuristic algorithm where each node adopts the most common 
        label among its neighbors through iterative updates.
        """
        )

        with st.spinner("Running Label Propagation community detection..."):
            lpa_df, lpa_totals = get_label_propagation_communities()

        # Display communities
        st.write(f"📈 Found {len(lpa_totals)} communities")

        # Show top communities
        top_communities = lpa_totals.head(10)

        for i, (community_id, total_score) in enumerate(top_communities.items()):
            community_members = lpa_df[lpa_df["communityId"] == community_id]
            member_count = len(community_members)

            with st.expander(
                f"🏷️ Community {i + 1} (ID: {community_id}) - {member_count} members (Total Score: {total_score:.1f})"
            ):
                display_community_members(lpa_df, community_id)

else:
    st.header(f"Community Details for: {selected_person}")

    # Louvain
    with st.spinner("Running Louvain community detection..."):
        louvain_df, louvain_totals = get_louvain_communities()

    louvain_community_id = search_person_community(selected_person, louvain_df)

    st.markdown("---")
    st.subheader("🏛️ Louvain Community")
    if louvain_community_id is not None:
        community_rank = list(louvain_totals.index).index(louvain_community_id) + 1
        total_score = louvain_totals[louvain_community_id]
        member_count = len(
            louvain_df[louvain_df["communityId"] == louvain_community_id]
        )

        expander_title = f"🏛️ Community {community_rank} (ID: {louvain_community_id}) - {member_count} members (Total Score: {total_score:.1f})"
        with st.expander(expander_title, expanded=True):
            display_community_members(louvain_df, louvain_community_id)
    else:
        st.error(f"❌ {selected_person} not found in any Louvain community.")

    # Label Propagation
    with st.spinner("Running Label Propagation community detection..."):
        lpa_df, lpa_totals = get_label_propagation_communities()

    lpa_community_id = search_person_community(selected_person, lpa_df)

    st.markdown("---")
    st.subheader("🏷️ Label Propagation Community")
    if lpa_community_id is not None:
        community_rank = list(lpa_totals.index).index(lpa_community_id) + 1
        total_score = lpa_totals[lpa_community_id]
        member_count = len(lpa_df[lpa_df["communityId"] == lpa_community_id])

        expander_title = f"🏷️ Community {community_rank} (ID: {lpa_community_id}) - {member_count} members (Total Score: {total_score:.1f})"
        with st.expander(expander_title, expanded=True):
            display_community_members(lpa_df, lpa_community_id)
    else:
        st.error(f"❌ {selected_person} not found in any Label Propagation community.")


# Footer with algorithm comparison
st.markdown("---")
st.markdown("""
### 🧠 Algorithm Comparison

**Louvain Algorithm:**
- ✅ Optimizes modularity for high-quality communities
- ✅ Hierarchical structure detection
- ✅ Good for finding well-defined research groups

**Label Propagation:**
- ✅ Very fast execution
- ✅ No prior assumptions about community structure
- ✅ Good for large networks and exploratory analysis
""")
