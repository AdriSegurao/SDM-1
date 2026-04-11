import argparse
from neo4j import GraphDatabase

# Find the top 3 most cited papers of each conference/workshop
QUERY_B1 = """
MATCH (p:Paper)-[:PUBLISHED_IN]->(:Edition)-[:EDITION_OF]->(cw:ConferenceWorkshop)
OPTIONAL MATCH (:Paper)-[:CITES]->(p)
WITH cw, p, count(*) AS citations
ORDER BY cw.name, citations DESC, p.title ASC
WITH cw, collect({paper: p.title, doi: p.DOI, citations: citations})[0..3] AS top3
RETURN cw.name AS conference, top3
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query the top 3 most cited papers for each conference/workshop in Neo4j."
    )
    parser.add_argument(
        "--uri",
        default="neo4j://127.0.0.1:7687",
        help="Neo4j connection URI"
    )
    parser.add_argument(
        "--user",
        default="neo4j",
        help="Neo4j username"
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Neo4j password"
    )
    return parser.parse_args()

def run_query(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            result = session.run(QUERY_B1)

            for record in result:
                print("Conference:", record["conference"])
                print("Top 3:")
                for paper in record["top3"]:
                    print(
                        f"  - Title: {paper['paper']}, "
                        f"DOI: {paper['doi']}, "
                        f"Citations: {paper['citations']}"
                    )
                print()

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)