#!/usr/bin/env python3

import argparse

from neo4j import GraphDatabase

NEO4J_URI = "bolt://127.0.0.1:7687"


def run_neo4j_import(
    user: str,
    password: str,
    database: str,
) -> None:
    print(f"Connecting to Neo4j at {NEO4J_URI} ...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(user, password))
    prefix = "file:///"

    def csv_ref(name: str) -> str:
        return f"{prefix}{name}"

    queries = [
        # 1. Constraints
        ("Creating constraint Author.authorId ...", "CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE"),
        ("Creating constraint Paper.DOI ...", "CREATE CONSTRAINT paper_doi IF NOT EXISTS FOR (p:Paper) REQUIRE p.DOI IS UNIQUE"),
        ("Creating constraint Keyword.term ...", "CREATE CONSTRAINT keyword_term IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE"),
        ("Creating constraint Journal.journalId ...", "CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE"),
        ("Creating constraint JournalVolume.journalVolumeId ...", "CREATE CONSTRAINT journal_volume_id IF NOT EXISTS FOR (jv:JournalVolume) REQUIRE jv.journalVolumeId IS UNIQUE"),
        ("Creating constraint Edition.editionId ...", "CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (e:Edition) REQUIRE e.editionId IS UNIQUE"),
        ("Creating constraint ConferenceWorkshop.venueId ...", "CREATE CONSTRAINT venue_id IF NOT EXISTS FOR (v:ConferenceWorkshop) REQUIRE v.venueId IS UNIQUE"),
        # 2. Node loading
        (
            "Loading Author nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("authors.csv")}' AS row
MERGE (a:Author {{authorId: row.authorId}})
SET a.name = row.name
""",
        ),
        (
            "Loading Paper nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("papers.csv")}' AS row
MERGE (p:Paper {{DOI: row.DOI}})
SET p.title = row.title,
    p.year = toInteger(row.year),
    p.abstract = row.abstract,
    p.pages = row.pages
""",
        ),
        (
            "Loading Keyword nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("keywords.csv")}' AS row
MERGE (:Keyword {{term: row.term}})
""",
        ),
        (
            "Loading Journal nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("journals.csv")}' AS row
MERGE (j:Journal {{journalId: row.journalId}})
SET j.name = row.name,
    j.issn = row.issn
""",
        ),
        (
            "Loading JournalVolume nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("journal_volumes.csv")}' AS row
MERGE (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
SET jv.year = toInteger(row.year),
    jv.volumeNumber = row.volumeNumber
""",
        ),
        (
            "Loading Edition nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("editions.csv")}' AS row
MERGE (e:Edition {{editionId: row.editionId}})
SET e.year = toInteger(row.year),
    e.city = row.city,
    e.proceedingsTitle = row.proceedingsTitle
""",
        ),
        (
            "Loading ConferenceWorkshop nodes ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("venues.csv")}' AS row
MERGE (v:ConferenceWorkshop {{venueId: row.venueId}})
SET v.name = row.name
""",
        ),
        # 3. Relationship loading
        (
            "Creating AUTHORED relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("authored.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (a)-[r:AUTHORED]->(p)
SET r.position = toInteger(row.position)
""",
        ),
        (
            "Creating CORRESPONDING_AUTHOR relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("corresponding_author.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (p)-[:CORRESPONDING_AUTHOR]->(a)
""",
        ),
        (
            "Creating REVIEWED relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("reviewed.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (a)-[:REVIEWED]->(p)
""",
        ),
        (
            "Creating CITES relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("cites.csv")}' AS row
MATCH (p1:Paper {{DOI: row.citingDOI}})
MATCH (p2:Paper {{DOI: row.citedDOI}})
MERGE (p1)-[:CITES]->(p2)
""",
        ),
        (
            "Creating HAS_KEYWORD relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("has_keyword.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (k:Keyword {{term: row.term}})
MERGE (p)-[:HAS_KEYWORD]->(k)
""",
        ),
        (
            "Creating PUBLISHED_IN (volume) relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("published_in_volume.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
MERGE (p)-[:PUBLISHED_IN]->(jv)
""",
        ),
        (
            "Creating PUBLISHED_IN (edition) relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("published_in_edition.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (e:Edition {{editionId: row.editionId}})
MERGE (p)-[:PUBLISHED_IN]->(e)
""",
        ),
        (
            "Creating VOLUME_OF relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("volume_of.csv")}' AS row
MATCH (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
MATCH (j:Journal {{journalId: row.journalId}})
MERGE (jv)-[:VOLUME_OF]->(j)
""",
        ),
        (
            "Creating EDITION_OF relationships ...",
            f"""
LOAD CSV WITH HEADERS FROM '{csv_ref("edition_of.csv")}' AS row
MATCH (e:Edition {{editionId: row.editionId}})
MATCH (v:ConferenceWorkshop {{venueId: row.venueId}})
MERGE (e)-[:EDITION_OF]->(v)
""",
        ),
    ]

    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            for message, query in queries:
                print(message)
                session.run(query).consume()
    finally:
        driver.close()

    print("Neo4j upload completed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload graph CSV files from Neo4j import directory into Neo4j."
    )
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j")
    args = parser.parse_args()

    run_neo4j_import(
        user=args.user,
        password=args.password,
        database=args.database,
    )


if __name__ == "__main__":
    main()
