import argparse
from neo4j import GraphDatabase

# Mark conference/workshop venues related to the Database community.
QUERY_C2_CONFERENCE = """
MATCH (c:Community {name: 'Database'})-[:DEFINED_BY]->(dbk:Keyword)
WITH c, collect(toLower(dbk.term)) AS db_keywords

MATCH (cw:ConferenceWorkshop)<-[:EDITION_OF]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:Keyword)
WITH c, db_keywords, cw, p, collect(toLower(k.term)) AS paper_keywords
WITH c, cw, p,
     any(k IN paper_keywords WHERE k IN db_keywords) AS is_db_paper
WITH c, cw,
     count(DISTINCT p) AS total_papers,
     count(DISTINCT CASE WHEN is_db_paper THEN p END) AS db_papers
WHERE total_papers > 0 AND 1.0 * db_papers / total_papers >= 0.9
MERGE (cw)-[:RELATED_TO]->(c)
RETURN cw.name AS venue,
       total_papers,
       db_papers,
       1.0 * db_papers / total_papers AS ratio
ORDER BY venue
"""

# Mark journal venues related to the Database community.
QUERY_C2_JOURNAL = """
MATCH (c:Community {name: 'Database'})-[:DEFINED_BY]->(dbk:Keyword)
WITH c, collect(toLower(dbk.term)) AS db_keywords

MATCH (j:Journal)<-[:VOLUME_OF]-(v:JournalVolume)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:Keyword)
WITH c, db_keywords, j, p, collect(toLower(k.term)) AS paper_keywords
WITH c, j, p,
     any(k IN paper_keywords WHERE k IN db_keywords) AS is_db_paper
WITH c, j,
     count(DISTINCT p) AS total_papers,
     count(DISTINCT CASE WHEN is_db_paper THEN p END) AS db_papers
WHERE total_papers > 0 AND 1.0 * db_papers / total_papers >= 0.9
MERGE (j)-[:RELATED_TO]->(c)
RETURN j.name AS venue,
       total_papers,
       db_papers,
       1.0 * db_papers / total_papers AS ratio
ORDER BY venue
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mark venues related to the Database community in Neo4j."
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

def print_results(title, result):
    found = False
    print(title)

    for record in result:
        found = True
        print(f"Venue: {record['venue']}")
        print(f"  Total papers: {record['total_papers']}")
        print(f"  Database papers: {record['db_papers']}")
        print(f"  Ratio: {record['ratio']:.3f}")
        print()

    if not found:
        print("  No matching venues found.\n")

def run_queries(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            conference_result = session.run(QUERY_C2_CONFERENCE)
            print_results("Conference/Workshop results:", conference_result)

            journal_result = session.run(QUERY_C2_JOURNAL)
            print_results("Journal results:", journal_result)

    except Exception as e:
        print(f"Error while connecting or executing the queries: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_queries(args.uri, args.user, args.password)