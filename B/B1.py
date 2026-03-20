from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "tu_password"

# Find the top 3 most cited papers of each conference/workshop
QUERY_B1 = """
MATCH (p:Paper)-[:PUBLISHED_IN]->(:Edition)-[:EDITION_OF]->(cw:ConferenceWorkshop)
OPTIONAL MATCH (:Paper)-[:CITES]->(p)
WITH cw, p, count(*) AS citations
ORDER BY cw.name, citations DESC, p.title ASC
WITH cw, collect({paper: p.title, doi: p.DOI, citations: citations})[0..3] AS top3
RETURN cw.name AS conference, top3
"""

def run_query():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    try:
        with driver.session() as session:
            result = session.run(QUERY_B1)

            for record in result:
                print("Conference:", record["conference"])
                print("Top 3:", record["top3"])
                print()

    finally:
        driver.close()


if __name__ == "__main__":
    run_query()