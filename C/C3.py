import argparse
from neo4j import GraphDatabase

# Find the top 100 papers published in venues(ConferenceWorkshop or Journal) related to the Database community.
QUERY_C3 = """
MATCH (:Community {name: 'Database'})<-[:RELATED_TO]-(venue)
MATCH (p:Paper)-[:PUBLISHED_IN]->(x)
WHERE (venue:ConferenceWorkshop AND (x)-[:EDITION_OF]->(venue))
   OR (venue:Journal AND (x)-[:VOLUME_OF]->(venue))
OPTIONAL MATCH (:Paper)-[:CITES]->(p)
WITH p, count(*) AS citations
ORDER BY citations DESC, p.title
LIMIT 100
SET p:TopDatabasePaper
SET p.dbCommunityCitations = citations
RETURN p.title AS title, p.DOI AS doi, citations
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Find the top 100 papers in Database community venues in Neo4j."
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
            result = session.run(QUERY_C3)

            found = False
            for record in result:
                found = True
                print(f"Title: {record['title']}")
                print(f"  DOI: {record['doi']}")
                print(f"  Citations: {record['citations']}")
                print()

            if not found:
                print("No top Database papers were found.")

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)