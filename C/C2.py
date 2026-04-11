import argparse
from neo4j import GraphDatabase

QUERY_C2_CONFERENCE = """
MATCH (c:Community {name: 'Database'})-[:DEFINED_BY]->(dbk:Keyword)
WITH collect(toLower(dbk.term)) AS db_terms

MATCH (cw:ConferenceWorkshop)<-[:EDITION_OF]-(:Edition)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:Keyword)
WITH db_terms, cw, p, collect(toLower(k.term)) AS paper_terms
WITH cw, p, any(t IN paper_terms WHERE t IN db_terms) AS is_db_paper
WITH cw,
     count(DISTINCT p) AS total,
     count(DISTINCT CASE WHEN is_db_paper THEN p END) AS db_related
WHERE total > 0 AND 1.0 * db_related / total >= 0.9
SET cw:DatabaseVenue
RETURN cw.name AS venue, total, db_related
ORDER BY venue
"""

QUERY_C2_JOURNAL = """
MATCH (c:Community {name: 'Database'})-[:DEFINED_BY]->(dbk:Keyword)
WITH collect(toLower(dbk.term)) AS db_terms

MATCH (j:Journal)<-[:VOLUME_OF]-(:JournalVolume)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:Keyword)
WITH db_terms, j, p, collect(toLower(k.term)) AS paper_terms
WITH j, p, any(t IN paper_terms WHERE t IN db_terms) AS is_db_paper
WITH j,
     count(DISTINCT p) AS total,
     count(DISTINCT CASE WHEN is_db_paper THEN p END) AS db_related
WHERE total > 0 AND 1.0 * db_related / total >= 0.9
SET j:DatabaseVenue
RETURN j.name AS venue, total, db_related
ORDER BY venue
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mark Database venues in Neo4j."
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687", help="Neo4j connection URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    return parser.parse_args()

def run_query(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            print("Conference/Workshop venues marked as DatabaseVenue:")
            result_conf = session.run(QUERY_C2_CONFERENCE)
            for record in result_conf:
                print(
                    f"  - {record['venue']} "
                    f"(db_related={record['db_related']}, total={record['total']})"
                )

            print("\nJournal venues marked as DatabaseVenue:")
            result_jour = session.run(QUERY_C2_JOURNAL)
            for record in result_jour:
                print(
                    f"  - {record['venue']} "
                    f"(db_related={record['db_related']}, total={record['total']})"
                )

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)