import argparse
from neo4j import GraphDatabase

# Create the Database community and connect it to the existing keywords in the graph.
QUERY_C1 = """
MERGE (c:Community {name: 'Database'})
WITH c, [
    'data management',
    'indexing',
    'data modeling',
    'big data',
    'data processing',
    'data storage',
    'data querying'
] AS terms
UNWIND terms AS term
MERGE (k:Keyword {term: term})
MERGE (c)-[:DEFINED_BY]->(k)
RETURN c.name AS community, collect(k.term) AS keywords
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Create the Database community and link it to its keywords in Neo4j."
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
            record = session.run(QUERY_C1).single()

            if record:
                print(f"Community created or matched: {record['community']}")
                print("Keywords linked to the community:")
                for keyword in sorted(record["keywords"]):
                    print(f"  - {keyword}")
            else:
                print("No result returned by the query.")

    except Exception as e:
        print(f"Error while connecting or executing the query: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    args = parse_args()
    run_query(args.uri, args.user, args.password)