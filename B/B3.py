from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "tu_password"


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

def run_query():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

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
    finally:
        driver.close()


if __name__ == "__main__":
    run_query()