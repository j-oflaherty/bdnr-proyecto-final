# %%
import start
from neo4j import GraphDatabase

# %%
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
# %%
with driver.session() as session:
    res = session.run(
        "MATCH (p1:Person)-[:AUTHOR_OF|CONTRIBUTOR_OF]->(w:Work)<-[:AUTHOR_OF|CONTRIBUTOR_OF]-(p2:Person) \
        WHERE elementId(p1) < elementId(p2) \
        RETURN id(p1) as id1, id(p2) as id2"
    )
    for record in res:
        print(record)
# %%
create_coauthorship_native_query = """
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
with driver.session() as session:
    if session.run("CALL gds.graph.exists('coauthor-graph')").single()["exists"]:
        res = session.run("CALL gds.graph.drop('coauthor-graph')")
    res = session.run(create_coauthorship_native_query)
    for record in res:
        print(record)

# %%
query = """
    MATCH (p1:Person)-[]->(w:Work)<-[]-(p2:Person)
    WHERE elementId(p1) < elementId(p2)
    RETURN p1.normalized_name as person1,
           p2.normalized_name as person2,
           count(w) as collaborations
    ORDER BY collaborations DESC
    LIMIT 10
    """

with driver.session() as session:
    result = session.run(query)
    print(" Top 10 collaboration pairs:")
    for record in result:
        print(
            f"  {record['person1']} <-> {record['person2']}: {record['collaborations']} works"
        )

# %% CENTRALITY
article_rank_query = """
CALL gds.articleRank.stream('coauthor-graph', {})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name as name, score
ORDER BY score DESC
LIMIT 20
"""
with driver.session() as session:
    result = session.run(article_rank_query)
    for record in result:
        print(record)

# %%
# an articulation point is a node whose removal increases the number of connected components in the graph
articulation_points_query = """
CALL gds.articulationPoints.stream('coauthor-graph')
YIELD nodeId, resultingComponents
RETURN gds.util.asNode(nodeId).normalized_name AS name, resultingComponents
ORDER BY name ASC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(articulation_points_query)
    for record in result:
        print(record)

# %%
# Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph. It is often used to find nodes that serve as a bridge from one part of a graph to another.
closeness_centrality_query = """
CALL gds.betweenness.stream('coauthor-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score
ORDER BY score DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(closeness_centrality_query)
    for record in result:
        print(record)

# %%
closeness_centrality_query = """
CALL gds.closeness.stream('coauthor-graph', {useWassermanFaust: true})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score
ORDER BY score DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(closeness_centrality_query)
    for record in result:
        print(record)

# %%
# Harmonic centrality was proposed as an alternative to closeness centrality, and therefore has similar use cases.
harmonic_centrality_query = """
CALL gds.closeness.harmonic.stream('coauthor-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score AS harmonic
ORDER BY harmonic DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(harmonic_centrality_query)
    for record in result:
        print(record)
# %%
# The Degree Centrality algorithm can be used to find popular nodes within a graph. The degree centrality measures the number of incoming or outgoing (or both) relationships from a node, which can be defined by the orientation of a relationship projection.
degree_centrality_query = """
CALL gds.degree.stream('coauthor-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score AS followers
ORDER BY followers DESC, name DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(degree_centrality_query)
    for record in result:
        print(record)

# %%
weight_degree_centrality_query = """
CALL gds.degree.stream('coauthor-graph', {relationshipWeightProperty: 'weight'})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score AS weight
ORDER BY weight DESC, name DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(weight_degree_centrality_query)
    for record in result:
        print(record)

# %%
# Eigenvector Centrality is an algorithm that measures the transitive influence of nodes. Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes. A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
eigenvector_centrality_query = """
CALL gds.eigenvector.stream('coauthor-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score AS eigenvector
ORDER BY eigenvector DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(eigenvector_centrality_query)
    for record in result:
        print(record)

# %%
# The PageRank algorithm measures the importance of each node within the graph, based on the number of incoming relationships and the importance of the corresponding source nodes.
page_rank_query = """
CALL gds.pageRank.stream('coauthor-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score AS pageRank
ORDER BY pageRank DESC
LIMIT 10
"""
with driver.session() as session:
    result = session.run(page_rank_query)
    for record in result:
        print(record)
