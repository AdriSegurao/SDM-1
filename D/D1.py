import argparse

from neo4j import GraphDatabase


GRAPH_NAME = "d1_paper_citation_graph"

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
            orientation: 'NATURAL'
        }
    }
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""

QUERY_PAGE_RANK = """
CALL gds.pageRank.stream(
    $graph_name,
    {
        maxIterations: $max_iterations,
        dampingFactor: $damping_factor
    }
)
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS p, score
OPTIONAL MATCH (citing:Paper)-[:CITES]->(p)
RETURN p.title AS paper,
       p.DOI AS doi,
       p.year AS year,
       score AS score,
       count(citing) AS incoming_citations
ORDER BY score DESC, incoming_citations DESC, paper ASC
LIMIT $limit
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run PageRank on the Paper citation graph using Neo4j GDS."
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687", help="Neo4j connection URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    parser.add_argument("--database", default="neo4j", help="Neo4j database name")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of ranked papers to print.",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=20,
        help="Maximum number of PageRank iterations.",
    )
    parser.add_argument(
        "--damping-factor",
        type=float,
        default=0.85,
        help="PageRank damping factor.",
    )
    return parser.parse_args()


def drop_graph_if_exists(session, graph_name):
    exists_record = session.run(QUERY_GRAPH_EXISTS, graph_name=graph_name).single()
    if exists_record and exists_record["exists"]:
        session.run(QUERY_DROP_GRAPH, graph_name=graph_name).consume()


def run_query(uri, user, password, database, limit, max_iterations, damping_factor):
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
                f"{graph_record['relationshipCount']} citation relationships."
            )
            print()
            print("Top papers by PageRank:")

            result = session.run(
                QUERY_PAGE_RANK,
                graph_name=GRAPH_NAME,
                limit=limit,
                max_iterations=max_iterations,
                damping_factor=damping_factor,
            )

            for rank, record in enumerate(result, start=1):
                print(
                    f"{rank:2}. Title: {record['paper']}\n"
                    f"    DOI: {record['doi']}\n"
                    f"    Year: {record['year']}\n"
                    f"    PageRank: {record['score']:.6f}\n"
                    f"    Incoming citations: {record['incoming_citations']}"
                )

            print()
            print("Interpretation:")
            print(
                "PageRank identifies papers that are important in the citation network, "
                "not only because they are cited often, but because influential papers cite them."
            )
            print(
                "In this domain, the top-ranked results can be interpreted as the papers with "
                "the highest research influence inside the DBLP subgraph loaded in Neo4j."
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
        args.max_iterations,
        args.damping_factor,
    )
