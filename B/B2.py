import argparse
from neo4j import GraphDatabase

# For each conference/workshop find its community:
# authors that have published in at least 4 different editions.
QUERY_B2 = """
MATCH (cw:ConferenceWorkshop)<-[:EDITION_OF]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)<-[:AUTHORED]-(a:Author)
WITH cw, a, count(DISTINCT e) AS numEditions
WHERE numEditions >= 4
RETURN cw.name AS conference,
       a.name AS author,
       a.authorId AS authorId,
       numEditions
ORDER BY conference, numEditions DESC, author
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query the author community for each conference/workshop in Neo4j."
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
            result = session.run(QUERY_B2)

            current_conference = None

            for record in result:
                conference = record["conference"]
                author = record["author"]
                author_id = record["authorId"]
                num_editions = record["numEditions"]

                if conference != current_conference:
                    current_conference = conference
                    print(f"\nConference/Workshop: {conference}")

                print(f"  - {author} (authorId={author_id}, editions={num_editions})")

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)