import argparse
from neo4j import GraphDatabase

# Find the h-index of the authors in your graph.
QUERY_B4 = """
MATCH (a:Author)-[:AUTHORED]->(p:Paper)
OPTIONAL MATCH (p)<-[:CITES]-(:Paper)
WITH a, p, count(*) AS citations
ORDER BY a.authorId, citations DESC
WITH a, collect(citations) AS cs
UNWIND range(0, size(cs)-1) AS i
WITH a, i + 1 AS rank, cs[i] AS citations
WHERE citations >= rank
RETURN a.authorId AS author_id,
       a.name AS author,
       max(rank) AS h_index
ORDER BY h_index DESC, author
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query the h-index of authors in Neo4j."
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
            result = session.run(QUERY_B4)

            for record in result:
                author_id = record["author_id"]
                author = record["author"]
                h_index = record["h_index"]

                print(f"Author: {author} (ID: {author_id})")
                print(f"  h-index: {h_index}")
                print()

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)