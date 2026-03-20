#!/usr/bin/env python3

import argparse
import functools
import os
import threading
import time
from contextlib import contextmanager
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator, List, Tuple

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import Neo4jError


REQUIRED_CSV_FILES = [
    "authors.csv",
    "papers.csv",
    "keywords.csv",
    "journals.csv",
    "journal_volumes.csv",
    "editions.csv",
    "venues.csv",
    "authored.csv",
    "corresponding_author.csv",
    "reviewed.csv",
    "cites.csv",
    "has_keyword.csv",
    "published_in_volume.csv",
    "published_in_edition.csv",
    "volume_of.csv",
    "edition_of.csv",
]


def check_required_csv(csv_dir: str) -> List[str]:
    missing = []
    for name in REQUIRED_CSV_FILES:
        if not os.path.isfile(os.path.join(csv_dir, name)):
            missing.append(name)
    return missing


def quote_cypher_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


class QuietCSVHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        # Silence per-request logs during LOAD CSV.
        return


@contextmanager
def serve_csv_directory(csv_dir: str, host: str = "127.0.0.1") -> Iterator[str]:
    handler = functools.partial(QuietCSVHandler, directory=csv_dir)
    server = ThreadingHTTPServer((host, 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        yield f"http://{host}:{port}/"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def ensure_database_exists(driver: Driver, database: str) -> None:
    if database.lower() in {"neo4j", "system"}:
        return

    db_name = quote_cypher_identifier(database)
    with driver.session(database="system") as session:
        session.run(f"CREATE DATABASE {db_name} IF NOT EXISTS").consume()

        deadline = time.time() + 30
        while time.time() < deadline:
            record = session.run(
                """
SHOW DATABASES YIELD name, currentStatus
WHERE name = $name
RETURN currentStatus AS status
""",
                name=database,
            ).single()
            if record and str(record["status"]).lower() == "online":
                return
            time.sleep(1)

    raise SystemExit(
        f"Database '{database}' did not become online after 30 seconds."
    )


def build_queries(csv_url_prefix: str = "file:///") -> List[Tuple[str, str]]:
    prefix = csv_url_prefix
    if not prefix.endswith("/"):
        prefix += "/"

    def csv_ref(name: str) -> str:
        return f"{prefix}{name}"

    return [
        ("Creating constraint Author.authorId ...", "CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE"),
        ("Creating constraint Paper.DOI ...", "CREATE CONSTRAINT paper_doi IF NOT EXISTS FOR (p:Paper) REQUIRE p.DOI IS UNIQUE"),
        ("Creating constraint Keyword.term ...", "CREATE CONSTRAINT keyword_term IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE"),
        ("Creating constraint Journal.journalId ...", "CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE"),
        ("Creating constraint JournalVolume.journalVolumeId ...", "CREATE CONSTRAINT journal_volume_id IF NOT EXISTS FOR (jv:JournalVolume) REQUIRE jv.journalVolumeId IS UNIQUE"),
        ("Creating constraint Edition.editionId ...", "CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (e:Edition) REQUIRE e.editionId IS UNIQUE"),
        ("Creating constraint ConferenceWorkshop.venueId ...", "CREATE CONSTRAINT venue_id IF NOT EXISTS FOR (v:ConferenceWorkshop) REQUIRE v.venueId IS UNIQUE"),
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


def execute_queries(driver: Driver, database: str, csv_url_prefix: str) -> None:
    queries = build_queries(csv_url_prefix=csv_url_prefix)
    with driver.session(database=database) as session:
        for message, query in queries:
            print(message)
            session.run(query).consume()


def run_neo4j_import(
    uri: str,
    user: str,
    password: str,
    database: str,
    csv_url_prefix: str | None,
    csv_dir: str,
    csv_http_host: str,
    create_db_if_missing: bool,
) -> None:
    print(f"Connecting to Neo4j at {uri} ...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        driver.verify_connectivity()

        if create_db_if_missing:
            try:
                ensure_database_exists(driver, database)
            except Neo4jError as exc:
                raise SystemExit(
                    f"Could not create/access database '{database}'. "
                    f"Use an admin user or run with --no-create-db-if-missing. "
                    f"Details: {exc}"
                ) from exc

        if csv_url_prefix:
            execute_queries(driver, database, csv_url_prefix)
        else:
            with serve_csv_directory(csv_dir=csv_dir, host=csv_http_host) as auto_prefix:
                print(f"Serving CSV files from {csv_dir} at {auto_prefix}")
                execute_queries(driver, database, auto_prefix)
    finally:
        driver.close()

    print("Neo4j upload completed.")


def main() -> None:
    default_csv_dir = str(Path(__file__).resolve().parent / "data" / "csv_graphmodel_data")
    parser = argparse.ArgumentParser(
        description="Upload graph CSV files (generated by FormatCSV.py) into Neo4j."
    )
    parser.add_argument(
        "--csv-dir",
        default=default_csv_dir,
        help="Directory containing CSV files generated by FormatCSV.py.",
    )
    parser.add_argument(
        "--csv-url-prefix",
        default=None,
        help=(
            "URL prefix used inside LOAD CSV. "
            "If omitted, a temporary local HTTP server is started for --csv-dir."
        ),
    )
    parser.add_argument(
        "--csv-http-host",
        default="127.0.0.1",
        help="Host/IP used by the temporary CSV HTTP server when --csv-url-prefix is not provided.",
    )
    parser.add_argument(
        "--neo4j-uri",
        default="bolt://127.0.0.1:7687",
        help="Neo4j URI. Use bolt:// for local single instance; neo4j:// for clustered/routing setups.",
    )
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--create-db-if-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create --database automatically when possible (requires admin privileges).",
    )
    args = parser.parse_args()

    csv_dir = str(Path(args.csv_dir).resolve())

    missing = check_required_csv(csv_dir)
    if missing:
        raise SystemExit("Missing CSV files in --csv-dir:\n- " + "\n- ".join(missing))

    run_neo4j_import(
        uri=args.neo4j_uri,
        user=args.user,
        password=args.password,
        database=args.database,
        csv_url_prefix=args.csv_url_prefix,
        csv_dir=csv_dir,
        csv_http_host=args.csv_http_host,
        create_db_if_missing=args.create_db_if_missing,
    )


if __name__ == "__main__":
    main()
