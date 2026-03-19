#! /usr/bin/env python3
from neo4j import GraphDatabase

# --- NEO4J CONFIGURATION ---
NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "adal2003"  # <--- Put your real password here

def run_neo4j_import(uri, user, password):
    print(f"🔗 Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # List of Cypher queries ordered logically
    queries = [
        # --- 1. INDEX CREATION (Crucial for fast import) ---
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal) REQUIRE j.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume) REQUIRE v.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Conference) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Edition) REQUIRE e.id IS UNIQUE",

        # --- 2. NODE LOADING ---
        print("Loading Paper Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (p:Paper {id: row.paper_id}) 
        SET p.title = row.title, p.pages = row.pages, p.doi = row.doi, p.abstract = row.abstract
        """,
        
        print("Loading Author Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///author_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (a:Author {name: row.name})
        """,
        
        print("Loading Journal Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///journal_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (j:Journal {name: row.journal_name})
        """,
        
        print("Loading Volume Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///volume_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (v:Volume {id: row.volume_id}) 
        SET v.number = row.number, v.year = toInteger(row.year)
        """,
        
        print("Loading Conference Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///conference_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (c:Conference {name: row.conf_name})
        """,
        
        print("Loading Edition Nodes..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///edition_node.csv' AS row FIELDTERMINATOR ';' 
        MERGE (e:Edition {id: row.edition_id}) 
        SET e.year = toInteger(row.year), e.title = row.title, e.number = row.edition_number
        """,
        
        # --- 3. RELATIONSHIP LOADING (in set t.tole=row.role we assign the edge property author or co-author) ---
        print("Creating WRITES Relationships (Author -> Paper)..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///writes_relation.csv' AS row FIELDTERMINATOR ';' 
        MATCH (a:Author {name: row.author_id})
        MATCH (p:Paper {id: row.paper_id}) 
        MERGE (a)-[r:WRITES]->(p) 
        SET r.role = row.role 
        """,
        
        print("Creating PUBLISHED_IN Relationships (Paper -> Volume/Edition)..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///published_in_relation.csv' AS row FIELDTERMINATOR ';' 
        MATCH (p:Paper {id: row.paper_id})
        MATCH (c) WHERE (c:Volume OR c:Edition) AND c.id = row.container_id 
        MERGE (p)-[:PUBLISHED_IN]->(c)
        """,
        
        print("Creating BELONGS_TO Relationships (Hierarchies)..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///belongs_to_relation.csv' AS row FIELDTERMINATOR ';' 
        MATCH (child) WHERE child.id = row.child_id
        MATCH (parent) WHERE parent.name = row.parent_id 
        MERGE (child)-[:BELONGS_TO]->(parent)
        """,
        
        print("Creating REVIEWS Relationships (Author -> Paper)..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///reviewed_by_relation.csv' AS row FIELDTERMINATOR ';' 
        MATCH (a:Author {name: row.author_name})
        MATCH (p:Paper {id: row.paper_id}) 
        MERGE (a)-[:REVIEWS]->(p)
        """,
        
        print("Creating CITES Relationships (Paper -> Paper)..."),
        """
        LOAD CSV WITH HEADERS FROM 'file:///cited_in_relation.csv' AS row FIELDTERMINATOR ';' 
        MATCH (ps:Paper {id: row.paper_id_source})
        MATCH (pt:Paper {id: row.paper_id_target}) 
        MERGE (ps)-[:CITES]->(pt)
        """
    ]

    with driver.session() as session:
        for q in queries:
            # Ignore the "print" statements included in the list for organization
            if isinstance(q, str): 
                session.run(q)
                
    driver.close()
    print("✅ Import completed in Neo4j! Go to your Browser to see the graph.")

if __name__ == "__main__":
    run_neo4j_import(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)