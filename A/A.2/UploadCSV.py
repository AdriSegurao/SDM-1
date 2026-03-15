#!/usr/bin/env python3

import argparse
import os
import subprocess
import tempfile
from typing import List


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


def build_load_cypher(csv_url_prefix: str = "file:///") -> str:
    prefix = csv_url_prefix
    if not prefix.endswith("/"):
        prefix += "/"

    def csv_ref(name: str) -> str:
        return f"{prefix}{name}"

    return f"""
CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE;
CREATE CONSTRAINT paper_doi IF NOT EXISTS FOR (p:Paper) REQUIRE p.DOI IS UNIQUE;
CREATE CONSTRAINT keyword_term IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE;
CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE;
CREATE CONSTRAINT journal_volume_id IF NOT EXISTS FOR (jv:JournalVolume) REQUIRE jv.journalVolumeId IS UNIQUE;
CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (e:Edition) REQUIRE e.editionId IS UNIQUE;
CREATE CONSTRAINT venue_id IF NOT EXISTS FOR (v:ConferenceWorkshop) REQUIRE v.venueId IS UNIQUE;

LOAD CSV WITH HEADERS FROM '{csv_ref("authors.csv")}' AS row
MERGE (a:Author {{authorId: row.authorId}})
SET a.name = row.name;

LOAD CSV WITH HEADERS FROM '{csv_ref("papers.csv")}' AS row
MERGE (p:Paper {{DOI: row.DOI}})
SET p.title = row.title,
    p.year = toInteger(row.year),
    p.abstract = row.abstract,
    p.pages = row.pages;

LOAD CSV WITH HEADERS FROM '{csv_ref("keywords.csv")}' AS row
MERGE (:Keyword {{term: row.term}});

LOAD CSV WITH HEADERS FROM '{csv_ref("journals.csv")}' AS row
MERGE (j:Journal {{journalId: row.journalId}})
SET j.name = row.name,
    j.issn = row.issn;

LOAD CSV WITH HEADERS FROM '{csv_ref("journal_volumes.csv")}' AS row
MERGE (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
SET jv.year = toInteger(row.year),
    jv.volumeNumber = row.volumeNumber;

LOAD CSV WITH HEADERS FROM '{csv_ref("editions.csv")}' AS row
MERGE (e:Edition {{editionId: row.editionId}})
SET e.year = toInteger(row.year),
    e.city = row.city,
    e.proceedingsTitle = row.proceedingsTitle;

LOAD CSV WITH HEADERS FROM '{csv_ref("venues.csv")}' AS row
MERGE (v:ConferenceWorkshop {{venueId: row.venueId}})
SET v.name = row.name;

LOAD CSV WITH HEADERS FROM '{csv_ref("authored.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (a)-[r:AUTHORED]->(p)
SET r.position = toInteger(row.position);

LOAD CSV WITH HEADERS FROM '{csv_ref("corresponding_author.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (p)-[:CORRESPONDING_AUTHOR]->(a);

LOAD CSV WITH HEADERS FROM '{csv_ref("reviewed.csv")}' AS row
MATCH (a:Author {{authorId: row.authorId}})
MATCH (p:Paper {{DOI: row.DOI}})
MERGE (a)-[:REVIEWED]->(p);

LOAD CSV WITH HEADERS FROM '{csv_ref("cites.csv")}' AS row
MATCH (p1:Paper {{DOI: row.citingDOI}})
MATCH (p2:Paper {{DOI: row.citedDOI}})
MERGE (p1)-[:CITES]->(p2);

LOAD CSV WITH HEADERS FROM '{csv_ref("has_keyword.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (k:Keyword {{term: row.term}})
MERGE (p)-[:HAS_KEYWORD]->(k);

LOAD CSV WITH HEADERS FROM '{csv_ref("published_in_volume.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
MERGE (p)-[:PUBLISHED_IN]->(jv);

LOAD CSV WITH HEADERS FROM '{csv_ref("published_in_edition.csv")}' AS row
MATCH (p:Paper {{DOI: row.DOI}})
MATCH (e:Edition {{editionId: row.editionId}})
MERGE (p)-[:PUBLISHED_IN]->(e);

LOAD CSV WITH HEADERS FROM '{csv_ref("volume_of.csv")}' AS row
MATCH (jv:JournalVolume {{journalVolumeId: row.journalVolumeId}})
MATCH (j:Journal {{journalId: row.journalId}})
MERGE (jv)-[:VOLUME_OF]->(j);

LOAD CSV WITH HEADERS FROM '{csv_ref("edition_of.csv")}' AS row
MATCH (e:Edition {{editionId: row.editionId}})
MATCH (v:ConferenceWorkshop {{venueId: row.venueId}})
MERGE (e)-[:EDITION_OF]->(v);
""".strip() + "\n"


def check_required_csv(csv_dir: str) -> List[str]:
    missing = []
    for name in REQUIRED_CSV_FILES:
        if not os.path.isfile(os.path.join(csv_dir, name)):
            missing.append(name)
    return missing


def write_load_cypher(out_path: str, csv_url_prefix: str) -> None:
    query = build_load_cypher(csv_url_prefix=csv_url_prefix)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(query)


def run_cypher_shell(
    cypher_shell: str,
    address: str,
    user: str,
    password: str,
    database: str,
    query: str,
) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".cypher", delete=False, encoding="utf-8") as tmp:
        tmp.write(query)
        tmp_path = tmp.name

    try:
        cmd = [
            cypher_shell,
            "-a",
            address,
            "-u",
            user,
            "-p",
            password,
            "-d",
            database,
            "-f",
            tmp_path,
        ]
        subprocess.run(cmd, check=True)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload graph CSV files (generated by FormatCSV.py) into Neo4j."
    )
    parser.add_argument("--csv-dir", default="neo4j_import")
    parser.add_argument("--cypher-out", default=None)
    parser.add_argument(
        "--csv-url-prefix",
        default="file:///",
        help="URL prefix used inside LOAD CSV. Example: file:/// or https://host/path/",
    )
    parser.add_argument("--run", action="store_true", help="Execute the query using cypher-shell.")
    parser.add_argument("--cypher-shell", default="cypher-shell")
    parser.add_argument("--address", default="bolt://localhost:7687")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database", default="neo4j")
    args = parser.parse_args()

    missing = check_required_csv(args.csv_dir) 
    if missing:
        raise SystemExit(
            "Missing CSV files in --csv-dir:\n- " + "\n- ".join(missing)
        )

    cypher_out = args.cypher_out or os.path.join(args.csv_dir, "load_a2.cypher")
    write_load_cypher(cypher_out, args.csv_url_prefix)
    print(f"Cypher upload script generated: {cypher_out}")

    if not args.run:
        print("Run with --run to execute it via cypher-shell.")
        return

    if not args.user or not args.password:
        raise SystemExit("When using --run, both --user and --password are required.")

    query = build_load_cypher(csv_url_prefix=args.csv_url_prefix)
    run_cypher_shell(
        cypher_shell=args.cypher_shell,
        address=args.address,
        user=args.user,
        password=args.password,
        database=args.database,
        query=query,
    )
    print("Neo4j upload completed.")


if __name__ == "__main__":
    main()
