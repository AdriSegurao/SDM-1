import argparse
import csv
from pathlib import Path

from neo4j import GraphDatabase


def read_csv_rows(csv_dir: Path, filename: str) -> list[dict[str, str]]:
    path = csv_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def run_query_in_batches(session, query: str, rows: list[dict[str, str]], batch_size: int = 1000) -> None:
    if not rows:
        return

    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        session.run(query, rows=batch).consume()
        print(f"  Processed {min(i + batch_size, total)}/{total}", flush=True)


def get_default_csv_dir() -> Path:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    return project_root / "data" / "csv_graphmodel_A2_data"


def run_neo4j_import(
    uri: str,
    user: str,
    password: str,
    database: str,
    csv_dir: Path,
    batch_size: int,
) -> None:
    print(f"Using CSV directory: {csv_dir}", flush=True)
    print(f"Connecting to Neo4j at {uri} ...", flush=True)
    driver = GraphDatabase.driver(uri, auth=(user, password))

    constraints = [
        ("Creating constraint Author.authorId ...", "CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE"),
        ("Creating constraint Paper.DOI ...", "CREATE CONSTRAINT paper_doi IF NOT EXISTS FOR (p:Paper) REQUIRE p.DOI IS UNIQUE"),
        ("Creating constraint Keyword.term ...", "CREATE CONSTRAINT keyword_term IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE"),
        ("Creating constraint Journal.journalId ...", "CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE"),
        ("Creating constraint JournalVolume.journalVolumeId ...", "CREATE CONSTRAINT journal_volume_id IF NOT EXISTS FOR (jv:JournalVolume) REQUIRE jv.journalVolumeId IS UNIQUE"),
        ("Creating constraint Edition.editionId ...", "CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (e:Edition) REQUIRE e.editionId IS UNIQUE"),
        ("Creating constraint ConferenceWorkshop.venueId ...", "CREATE CONSTRAINT venue_id IF NOT EXISTS FOR (v:ConferenceWorkshop) REQUIRE v.venueId IS UNIQUE"),
    ]

    node_loads = [
        (
            "Loading Author nodes ...",
            "authors.csv",
            """
            UNWIND $rows AS row
            MERGE (a:Author {authorId: row.authorId})
            SET a.name = row.name
            """,
        ),
        (
            "Loading Paper nodes ...",
            "papers.csv",
            """
            UNWIND $rows AS row
            MERGE (p:Paper {DOI: row.DOI})
            SET p.title = row.title,
                p.year = toInteger(row.year),
                p.abstract = row.abstract,
                p.pages = row.pages
            """,
        ),
        (
            "Loading Keyword nodes ...",
            "keywords.csv",
            """
            UNWIND $rows AS row
            MERGE (:Keyword {term: row.term})
            """,
        ),
        (
            "Loading Journal nodes ...",
            "journals.csv",
            """
            UNWIND $rows AS row
            MERGE (j:Journal {journalId: row.journalId})
            SET j.name = row.name,
                j.issn = row.issn
            """,
        ),
        (
            "Loading JournalVolume nodes ...",
            "journal_volumes.csv",
            """
            UNWIND $rows AS row
            MERGE (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
            SET jv.year = toInteger(row.year),
                jv.volumeNumber = row.volumeNumber
            """,
        ),
        (
            "Loading Edition nodes ...",
            "editions.csv",
            """
            UNWIND $rows AS row
            MERGE (e:Edition {editionId: row.editionId})
            SET e.year = toInteger(row.year),
                e.city = row.city,
                e.proceedingsTitle = row.proceedingsTitle
            """,
        ),
        (
            "Loading ConferenceWorkshop nodes ...",
            "venues.csv",
            """
            UNWIND $rows AS row
            MERGE (v:ConferenceWorkshop {venueId: row.venueId})
            SET v.name = row.name
            """,
        ),
    ]

    relationship_loads = [
        (
            "Creating AUTHORED relationships ...",
            "authored.csv",
            """
            UNWIND $rows AS row
            MATCH (a:Author {authorId: row.authorId})
            MATCH (p:Paper {DOI: row.DOI})
            MERGE (a)-[r:AUTHORED]->(p)
            SET r.position = toInteger(row.position)
            """,
        ),
        (
            "Creating CORRESPONDING_AUTHOR relationships ...",
            "corresponding_author.csv",
            """
            UNWIND $rows AS row
            MATCH (a:Author {authorId: row.authorId})
            MATCH (p:Paper {DOI: row.DOI})
            MERGE (p)-[:CORRESPONDING_AUTHOR]->(a)
            """,
        ),
        (
            "Creating REVIEWED relationships ...",
            "reviewed.csv",
            """
            UNWIND $rows AS row
            MATCH (a:Author {authorId: row.authorId})
            MATCH (p:Paper {DOI: row.DOI})
            MERGE (a)-[:REVIEWED]->(p)
            """,
        ),
        (
            "Creating CITES relationships ...",
            "cites.csv",
            """
            UNWIND $rows AS row
            MATCH (p1:Paper {DOI: row.citingDOI})
            MATCH (p2:Paper {DOI: row.citedDOI})
            MERGE (p1)-[:CITES]->(p2)
            """,
        ),
        (
            "Creating HAS_KEYWORD relationships ...",
            "has_keyword.csv",
            """
            UNWIND $rows AS row
            MATCH (p:Paper {DOI: row.DOI})
            MATCH (k:Keyword {term: row.term})
            MERGE (p)-[:HAS_KEYWORD]->(k)
            """,
        ),
        (
            "Creating PUBLISHED_IN (volume) relationships ...",
            "published_in_volume.csv",
            """
            UNWIND $rows AS row
            MATCH (p:Paper {DOI: row.DOI})
            MATCH (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
            MERGE (p)-[:PUBLISHED_IN]->(jv)
            """,
        ),
        (
            "Creating PUBLISHED_IN (edition) relationships ...",
            "published_in_edition.csv",
            """
            UNWIND $rows AS row
            MATCH (p:Paper {DOI: row.DOI})
            MATCH (e:Edition {editionId: row.editionId})
            MERGE (p)-[:PUBLISHED_IN]->(e)
            """,
        ),
        (
            "Creating VOLUME_OF relationships ...",
            "volume_of.csv",
            """
            UNWIND $rows AS row
            MATCH (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
            MATCH (j:Journal {journalId: row.journalId})
            MERGE (jv)-[:VOLUME_OF]->(j)
            """,
        ),
        (
            "Creating EDITION_OF relationships ...",
            "edition_of.csv",
            """
            UNWIND $rows AS row
            MATCH (e:Edition {editionId: row.editionId})
            MATCH (v:ConferenceWorkshop {venueId: row.venueId})
            MERGE (e)-[:EDITION_OF]->(v)
            """,
        ),
    ]

    try:
        driver.verify_connectivity()

        with driver.session(database=database) as session:
            for message, query in constraints:
                print(message, flush=True)
                session.run(query).consume()

            for message, filename, query in node_loads:
                print(message, flush=True)
                rows = read_csv_rows(csv_dir, filename)
                run_query_in_batches(session, query, rows, batch_size=batch_size)

            for message, filename, query in relationship_loads:
                print(message, flush=True)
                rows = read_csv_rows(csv_dir, filename)
                run_query_in_batches(session, query, rows, batch_size=batch_size)

    finally:
        driver.close()

    print("Neo4j upload completed.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload graph CSV files from data/csv_graphmodel_A2_data into Neo4j using Python + UNWIND."
    )
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of CSV rows sent to Neo4j per batch.",
    )
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7687")

    args = parser.parse_args()

    csv_dir = get_default_csv_dir()

    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV directory not found: {csv_dir}")

    run_neo4j_import(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
        csv_dir=csv_dir,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()