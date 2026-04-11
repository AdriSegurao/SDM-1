import argparse
from neo4j import GraphDatabase

QUERY_C3 = """
MATCH (p:Paper)
WHERE
  EXISTS {
    MATCH (p)-[:PUBLISHED_IN]->(:Edition)-[:EDITION_OF]->(cw:ConferenceWorkshop)
    WHERE cw:DatabaseVenue
  }
  OR
  EXISTS {
    MATCH (p)-[:PUBLISHED_IN]->(:JournalVolume)-[:VOLUME_OF]->(j:Journal)
    WHERE j:DatabaseVenue
  }

MATCH (citing:Paper)-[:CITES]->(p)
MATCH (citing)-[:HAS_KEYWORD]->(k:Keyword)<-[:DEFINED_BY]-(:Community {name: 'Database'})
WITH p, count(DISTINCT citing) AS db_citations
ORDER BY db_citations DESC, p.title ASC
LIMIT 100
SET p:TopDBPaper
RETURN p.title AS paper, p.DOI AS doi, db_citations
ORDER BY db_citations DESC, paper ASC
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mark the top-100 Database papers in Neo4j."
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687", help="Neo4j connection URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    return parser.parse_args()

def run_query(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            result = session.run(QUERY_C3)

            print("Top Database papers:")
            for i, record in enumerate(result, start=1):
                print(
                    f"{i:3}. Title: {record['paper']}, "
                    f"DOI: {record['doi']}, "
                    f"DB citations: {record['db_citations']}"
                )

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)