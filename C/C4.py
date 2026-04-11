import argparse
from neo4j import GraphDatabase

# Mark potential reviewers and gurus for the Database community.
QUERY_C4 = """
MATCH (a:Author)-[:AUTHORED]->(p:TopDatabasePaper)
MATCH (c:Community {name: 'Database'})
WITH a, c, count(DISTINCT p) AS top_papers
MERGE (a)-[:POTENTIAL_REVIEWER_FOR]->(c)
SET a:PotentialReviewer,
    a.topDatabasePapers = top_papers
WITH a, top_papers
WHERE top_papers >= 2
SET a:Guru
RETURN a.name AS author,
       a.authorId AS authorId,
       top_papers,
       true AS is_guru
ORDER BY top_papers DESC, author
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mark potential reviewers and gurus for the Database community in Neo4j."
    )
    parser.add_argument(
        "--uri",
        default="bolt://127.0.0.1:7687",
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
            result = session.run(QUERY_C4)

            found = False
            for record in result:
                found = True
                author = record["author"]
                author_id = record["authorId"]
                top_papers = record["top_papers"]

                print(f"Author: {author} (ID: {author_id})")
                print(f"  Top Database papers: {top_papers}")
                print("  Guru: True")
                print()

            if not found:
                print("No gurus were found.")

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)