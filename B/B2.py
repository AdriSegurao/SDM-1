from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "tu_password"


# For each conference/workshop find its community: i.e., those authors that have published papers on that conference/workshop in, at least, 4 different editions.
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

def run_query():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

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

    finally:
        driver.close()


if __name__ == "__main__":
    run_query()