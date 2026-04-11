import argparse

from neo4j import GraphDatabase


GRAPH_NAME = "d2_paper_community_graph"

QUERY_GDS_CHECK = """
CALL gds.list()
YIELD name
RETURN count(name) AS algorithm_count
"""

QUERY_GRAPH_EXISTS = """
CALL gds.graph.exists($graph_name)
YIELD exists
RETURN exists
"""

QUERY_DROP_GRAPH = """
CALL gds.graph.drop($graph_name, false)
YIELD graphName
RETURN graphName
"""

QUERY_CREATE_GRAPH = """
CALL gds.graph.project(
    $graph_name,
    'Paper',
    {
        CITES: {
            orientation: 'UNDIRECTED'
        }
    }
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""

QUERY_LOUVAIN_STATS = """
CALL gds.louvain.stats($graph_name)
YIELD communityCount, modularity
RETURN communityCount, modularity
"""

QUERY_LOUVAIN = """
CALL gds.louvain.stream($graph_name)
YIELD nodeId, communityId
WITH communityId, gds.util.asNode(nodeId) AS p
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:Keyword)
RETURN communityId,
       count(DISTINCT p) AS size,
       min(p.year) AS first_year,
       max(p.year) AS last_year,
       collect(DISTINCT p.title)[0..$sample_size] AS sample_papers,
       collect(DISTINCT k.term)[0..$keyword_sample] AS sample_keywords
ORDER BY size DESC, communityId ASC
LIMIT $limit
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Louvain on the Paper citation graph using Neo4j GDS."
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687", help="Neo4j connection URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    parser.add_argument("--database", default="neo4j", help="Neo4j database name")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of communities to print.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of paper titles shown per community.",
    )
    parser.add_argument(
        "--keyword-sample",
        type=int,
        default=5,
        help="Number of keywords shown per community.",
    )
    return parser.parse_args()


def drop_graph_if_exists(session, graph_name):
    exists_record = session.run(QUERY_GRAPH_EXISTS, graph_name=graph_name).single()
    if exists_record and exists_record["exists"]:
        session.run(QUERY_DROP_GRAPH, graph_name=graph_name).consume()


def run_query(uri, user, password, database, limit, sample_size, keyword_sample):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    graph_created = False

    try:
        with driver.session(database=database) as session:
            check_record = session.run(QUERY_GDS_CHECK).single()
            print(f"GDS available. Listed algorithms: {check_record['algorithm_count']}")

            drop_graph_if_exists(session, GRAPH_NAME)

            graph_record = session.run(
                QUERY_CREATE_GRAPH,
                graph_name=GRAPH_NAME,
            ).single()
            graph_created = True

            print(
                f"Projected graph '{graph_record['graphName']}' "
                f"with {graph_record['nodeCount']} papers and "
                f"{graph_record['relationshipCount']} undirected citation links."
            )

            stats_record = session.run(
                QUERY_LOUVAIN_STATS,
                graph_name=GRAPH_NAME,
            ).single()
            print(
                f"Louvain detected {stats_record['communityCount']} communities "
                f"(modularity={stats_record['modularity']:.6f})."
            )
            print()
            print("Largest communities:")

            result = session.run(
                QUERY_LOUVAIN,
                graph_name=GRAPH_NAME,
                limit=limit,
                sample_size=sample_size,
                keyword_sample=keyword_sample,
            )

            for record in result:
                print(
                    f"Community {record['communityId']}:\n"
                    f"    Size: {record['size']} papers\n"
                    f"    Years: {record['first_year']} - {record['last_year']}\n"
                    f"    Sample papers: {record['sample_papers']}\n"
                    f"    Sample keywords: {record['sample_keywords']}"
                )

            print()
            print("Interpretation:")
            print(
                "Louvain groups papers that are more densely connected through citations than with the rest of the graph."
            )
            print(
                "In the research-publication domain, these communities can be interpreted as subfields or research lines, "
                "and the sample keywords help explain the topic of each detected cluster."
            )

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")
        print(
            "If the error mentions 'gds', verify that the Neo4j Graph Data Science plugin "
            "is installed and enabled in your Neo4j instance."
        )

    finally:
        try:
            with driver.session(database=database) as session:
                if graph_created:
                    drop_graph_if_exists(session, GRAPH_NAME)
        except Exception:
            pass
        driver.close()


if __name__ == "__main__":
    args = parse_args()
    run_query(
        args.uri,
        args.user,
        args.password,
        args.database,
        args.limit,
        args.sample_size,
        args.keyword_sample,
    )
