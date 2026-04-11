import argparse
from neo4j import GraphDatabase

# Find the impact factor of the journals in your graph.
QUERY_B3 = """
MATCH (j:Journal)<-[:VOLUME_OF]-(v:JournalVolume)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)<-[:CITES]-(citing:Paper)
WITH j.journalId AS journal,
     count(DISTINCT p) AS paper_count,
     count(citing) AS total_citations
RETURN journal,
       total_citations * 1.0 / paper_count AS impact_factor,
       total_citations,
       paper_count AS total_papers
ORDER BY impact_factor DESC
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query the impact factor of journals in Neo4j."
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
            result = session.run(QUERY_B3)

            for record in result:
                journal = record["journal"]
                total_citations = record["total_citations"]
                total_papers = record["total_papers"]
                impact_factor = record["impact_factor"]

                print(f"Journal: {journal}")
                print(f"  Citations: {total_citations}")
                print(f"  Published papers: {total_papers}")
                print(f"  Impact factor: {impact_factor:.3f}")
                print()

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)