import argparse
from neo4j import GraphDatabase

QUERY_C4 = """
MATCH (a:Author)-[:AUTHORED]->(p:TopDBPaper)
WITH a, count(DISTINCT p) AS top_paper_count
SET a:PotentialReviewer
WITH a, top_paper_count
WHERE top_paper_count >= 2
SET a:Guru
RETURN a.name AS author, top_paper_count,
       a:PotentialReviewer AS potential_reviewer,
       a:Guru AS guru
ORDER BY top_paper_count DESC, author ASC
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mark potential reviewers and gurus in Neo4j."
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687", help="Neo4j connection URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    return parser.parse_args()

def run_query(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            result = session.run(QUERY_C4)

            print("Potential reviewers / Gurus:")
            for record in result:
                print(
                    f"  - Author: {record['author']}, "
                    f"Top papers: {record['top_paper_count']}, "
                    f"PotentialReviewer: {record['potential_reviewer']}, "
                    f"Guru: {record['guru']}"
                )

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)