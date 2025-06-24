# %%
import pandas
import start
from neo4j import GraphDatabase

# %%
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
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
kcore_query = """
CALL gds.kcore.stream('coauthor-graph', {})
YIELD nodeId, coreValue
RETURN gds.util.asNode(nodeId).normalized_name AS name, coreValue
"""
with driver.session() as session:
    result = session.run(kcore_query).to_df()
result
# %%
node_degree_query = """
CALL gds.degree.stream('coauthor-graph', {})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).normalized_name AS name, score
"""
with driver.session() as session:
    popularity = session.run(node_degree_query).to_df()
# %%
result = result.merge(popularity, on="name", how="left")
result
# %%
for coreValue, group in result.groupby("coreValue"):
    print(f"Core value: {coreValue}")
    print(f"Number of nodes: {len(group)}")
    for name in group.sort_values("score", ascending=False).to_dict(orient="records")[
        :10
    ]:
        print(f"  {name}")
    print("-" * 100)
# %%
label_propagation_query = """
CALL gds.labelPropagation.stream('coauthor-graph', {})
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).normalized_name AS name, communityId
"""
with driver.session() as session:
    result = session.run(label_propagation_query).to_df()
result = result.merge(popularity, on="name", how="left")
result
# %%
community_totals = (
    result.groupby("communityId")["score"].sum().sort_values(ascending=False)
)

for i, communityId in enumerate(community_totals.index):
    print(
        f"Community ID: {communityId} (Total Score: {community_totals[communityId]:.2f})"
    )

    community_members = result[result["communityId"] == communityId].sort_values(
        "score", ascending=False
    )

    for member in community_members.to_dict(orient="records")[:10]:
        print(f"  - {member['name']}: {member['score']:.2f}")
    if i > 10:
        break
# %%
louvain_query = """
CALL gds.louvain.stream('coauthor-graph', {})
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).normalized_name AS name, communityId
"""
with driver.session() as session:
    result = session.run(louvain_query).to_df()
result = result.merge(popularity, on="name", how="left")
result
# %% TREMENDAS COMUNIDADES
community_totals = (
    result.groupby("communityId")["score"].sum().sort_values(ascending=False)
)

for i, communityId in enumerate(community_totals.index):
    print(
        f"Community ID: {communityId} (Total Score: {community_totals[communityId]:.2f})"
    )
    community_members = result[result["communityId"] == communityId].sort_values(
        "score", ascending=False
    )

    for member in community_members.to_dict(orient="records")[:10]:
        print(f"  - {member['name']}: {member['score']:.2f}")
    if i > 10:
        break

# %%
scc_query = """
CALL gds.scc.stream('coauthor-graph', {})
YIELD nodeId, componentId
RETURN gds.util.asNode(nodeId).normalized_name AS name, componentId
"""
with driver.session() as session:
    result = session.run(scc_query).to_df()
result = result.merge(popularity, on="name", how="left")
result
# %% Bad results
component_totals = (
    result.groupby("componentId")["score"].sum().sort_values(ascending=False)
)

for i, componentId in enumerate(component_totals.index):
    print(
        f"Component ID: {componentId} (Total Score: {component_totals[componentId]:.2f})"
    )
    component_members = result[result["componentId"] == componentId].sort_values(
        "score", ascending=False
    )

    for member in component_members.to_dict(orient="records")[:10]:
        print(f"  - {member['name']}: {member['score']:.2f}")
    if i > 10:
        break
# %%
sllpa_query = """
CALL gds.sllpa.stream('coauthor-graph', {maxIterations: 100, minAssociationStrength: 0.1})
YIELD nodeId, values
RETURN gds.util.asNode(nodeId).normalized_name AS name, values.communityIds AS communityIds
ORDER BY name ASC
"""
with driver.session() as session:
    result = session.run(sllpa_query).to_df()
result = result.merge(popularity, on="name", how="left")
result
# %%
result = result.explode("communityIds")
result
# %%
community_totals = (
    result.groupby("communityIds")["score"].sum().sort_values(ascending=False)
)

for i, communityId in enumerate(community_totals.index):
    print(
        f"Community ID: {communityId} (Total Score: {community_totals[communityId]:.2f})"
    )
    community_members = result[result["communityIds"] == communityId].sort_values(
        "score", ascending=False
    )

    for member in community_members.to_dict(orient="records")[:10]:
        print(f"  - {member['name']}: {member['score']:.2f}")
    if i > 10:
        break
# %%
result
