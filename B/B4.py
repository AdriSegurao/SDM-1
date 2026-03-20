from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "tu_password"


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

def run_query():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

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

    finally:
        driver.close()


if __name__ == "__main__":
    run_query()