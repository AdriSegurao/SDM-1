#!/usr/bin/env python3

import argparse
import csv
import os
import random
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


KEYWORD_FALLBACKS = {
    "graph": ["graph processing", "property graph"],
    "database": ["data management", "data querying"],
    "query": ["data querying"],
    "index": ["indexing"],
    "model": ["data modeling"],
    "big data": ["big data"],
    "storage": ["data storage"],
    "process": ["data processing"],
}


def slug(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def split_multi(value: str) -> List[str]:
    if value is None:
        return []
    value = str(value).strip()
    if not value:
        return []
    return [part.strip() for part in value.split("|") if part.strip()]


def guess_doi(ee_value: str) -> str:
    values = split_multi(ee_value)
    for v in values:
        low = v.lower()
        if "doi.org/" in low:
            return v.split("doi.org/")[-1].strip("/")
        if re.match(r"^10\.\S+", v):
            return v
    return ""


def parse_header_types(header_path: str) -> List[str]:
    with open(header_path, "r", encoding="utf-8") as f:
        line = f.readline().strip()
    cols = []
    for item in line.split(";"):
        col = item.split(":", 1)[0].strip()
        cols.append(col)
    return cols


def read_intermediate_with_header(
    data_path: str,
    header_path: str,
    limit: Optional[int] = None,
) -> List[Dict[str, str]]:
    columns = parse_header_types(header_path)
    rows: List[Dict[str, str]] = []

    with open(data_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for raw in reader:
            if not raw:
                continue

            if len(raw) < len(columns):
                raw = raw + [""] * (len(columns) - len(raw))
            elif len(raw) > len(columns):
                raw = raw[:len(columns)]

            rows.append(dict(zip(columns, raw)))

            if limit is not None and len(rows) >= limit:
                break

    return rows


class GraphBuilder:
    def __init__(self, reviewers_per_paper: int = 3, seed: int = 42) -> None:
        self.random = random.Random(seed)
        self.reviewers_per_paper = reviewers_per_paper

        self.authors: Dict[str, Dict[str, Any]] = {}
        self.papers: Dict[str, Dict[str, Any]] = {}
        self.keywords: Dict[str, Dict[str, Any]] = {}
        self.journals: Dict[str, Dict[str, Any]] = {}
        self.journal_volumes: Dict[str, Dict[str, Any]] = {}
        self.proceedings: Dict[str, Dict[str, Any]] = {}
        self.editions: Dict[str, Dict[str, Any]] = {}
        self.venues: Dict[str, Dict[str, Any]] = {}

        self.authored: Set[Tuple[str, str, int]] = set()
        self.corresponding_author: Set[Tuple[str, str]] = set()
        self.reviewed: Set[Tuple[str, str]] = set()
        self.cites: Set[Tuple[str, str]] = set()
        self.has_keyword: Set[Tuple[str, str]] = set()
        self.published_in_volume: Set[Tuple[str, str]] = set()
        self.published_in_proceedings: Set[Tuple[str, str]] = set()
        self.volume_of: Set[Tuple[str, str]] = set()
        self.for_edition: Set[Tuple[str, str]] = set()
        self.edition_of: Set[Tuple[str, str]] = set()

        self.paper_authors: Dict[str, Set[str]] = defaultdict(set)
        self.all_author_ids: Set[str] = set()

        self.proc_meta_by_key: Dict[Tuple[str, int], Dict[str, str]] = {}
        self.paper_lookup: Dict[str, str] = {}

    def infer_keywords(self, title: str) -> List[str]:
        found = set()
        low = (title or "").lower()
        for trigger, kws in KEYWORD_FALLBACKS.items():
            if trigger in low:
                found.update(kws)
        if not found:
            found.add("general research")
        return sorted(found)

    def add_author(self, author_name: str) -> str:
        author_id = f"author_{slug(author_name)}"
        self.authors.setdefault(author_id, {"authorId": author_id, "name": author_name})
        self.all_author_ids.add(author_id)
        return author_id

    def add_keyword(self, term: str) -> None:
        term = (term or "").strip().lower()
        if term:
            self.keywords.setdefault(term, {"term": term})

    def make_paper_id(self, row: Dict[str, str], fallback_prefix: str) -> str:
        key = (row.get("key") or "").strip()
        title = (row.get("title") or "untitled").strip()
        year = safe_int(row.get("year"), 2000)
        if key:
            return f"paper_{slug(key)}"
        return f"paper_{slug(fallback_prefix + '-' + title + '-' + str(year))}"

    def add_citation_stub(self, raw_cite: str) -> str:
        cited_id = f"paper_{slug(raw_cite)}"
        self.papers.setdefault(
            cited_id,
            {
                "paperId": cited_id,
                "doi": "",
                "title": raw_cite,
                "year": 2000,
                "abstract": f"Synthetic abstract for {raw_cite}.",
                "pages": "unknown",
            },
        )
        return cited_id

    def process_article_row(self, row: Dict[str, str]) -> None:
        key = (row.get("key") or "").strip()
        if key.startswith("dblpnote/"):
            return

        title = (row.get("title") or "").strip()
        journal_name = (row.get("journal") or "").strip()

        if not title or not journal_name:
            return

        paper_id = self.make_paper_id(row, "article")
        year = safe_int(row.get("year"), 2000)
        pages = (row.get("pages") or "unknown").strip()
        doi = guess_doi(row.get("ee", ""))
        volume = str(row.get("volume") or "1").strip()

        self.papers.setdefault(
            paper_id,
            {
                "paperId": paper_id,
                "doi": doi,
                "title": title,
                "year": year,
                "abstract": f"Synthetic abstract for {title}.",
                "pages": pages,
            },
        )

        if key:
            self.paper_lookup[key] = paper_id

        authors = split_multi(row.get("author", ""))
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_id, pos))
            self.paper_authors[paper_id].add(author_id)

        if authors:
            self.corresponding_author.add((f"author_{slug(authors[0])}", paper_id))

        for kw in self.infer_keywords(title):
            self.add_keyword(kw)
            self.has_keyword.add((paper_id, kw))

        journal_id = f"journal_{slug(journal_name)}"
        jv_id = f"jv_{slug(journal_name)}_{year}_{slug(volume)}"

        self.journals.setdefault(
            journal_id,
            {"journalId": journal_id, "name": journal_name, "issn": "unknown"},
        )
        self.journal_volumes.setdefault(
            jv_id,
            {"journalVolumeId": jv_id, "year": year, "volume": volume},
        )

        self.volume_of.add((jv_id, journal_id))
        self.published_in_volume.add((paper_id, jv_id))

        for cited in split_multi(row.get("cite", "")):
            if cited == "..." or cited.lower() == "omitted":
                continue
            cited_paper_id = self.paper_lookup.get(cited)
            if not cited_paper_id:
                cited_paper_id = self.add_citation_stub(cited)
            self.cites.add((paper_id, cited_paper_id))

    def process_inproceedings_row(self, row: Dict[str, str]) -> None:
        key = (row.get("key") or "").strip()
        if key.startswith("dblpnote/"):
            return

        title = (row.get("title") or "").strip()
        venue_name = (row.get("booktitle") or "").strip()

        if not title or not venue_name:
            return

        paper_id = self.make_paper_id(row, "inproceedings")
        year = safe_int(row.get("year"), 2000)
        pages = (row.get("pages") or "unknown").strip()
        doi = guess_doi(row.get("ee", ""))

        self.papers.setdefault(
            paper_id,
            {
                "paperId": paper_id,
                "doi": doi,
                "title": title,
                "year": year,
                "abstract": f"Synthetic abstract for {title}.",
                "pages": pages,
            },
        )

        if key:
            self.paper_lookup[key] = paper_id

        authors = split_multi(row.get("author", ""))
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_id, pos))
            self.paper_authors[paper_id].add(author_id)

        if authors:
            self.corresponding_author.add((f"author_{slug(authors[0])}", paper_id))

        for kw in self.infer_keywords(title):
            self.add_keyword(kw)
            self.has_keyword.add((paper_id, kw))

        venue_id = f"venue_{slug(venue_name)}"
        edition_id = f"edition_{slug(venue_name)}_{year}"
        proceeding_id = f"proc_{slug(venue_name)}_{year}"

        proc_meta = self.proc_meta_by_key.get((venue_name.lower(), year), {})
        city = proc_meta.get("city", "Unknown City")
        isbn = proc_meta.get("isbn", "unknown")
        proc_title = proc_meta.get("proceeding_title", f"Proceedings of {venue_name} {year}")

        self.venues.setdefault(venue_id, {"venueId": venue_id, "name": venue_name})
        self.editions.setdefault(edition_id, {"editionId": edition_id, "year": year, "city": city})
        self.proceedings.setdefault(
            proceeding_id,
            {"proceedingId": proceeding_id, "title": proc_title, "isbn": isbn},
        )

        self.edition_of.add((edition_id, venue_id))
        self.for_edition.add((proceeding_id, edition_id))
        self.published_in_proceedings.add((paper_id, proceeding_id))

        for cited in split_multi(row.get("cite", "")):
            if cited == "..." or cited.lower() == "omitted":
                continue
            cited_paper_id = self.paper_lookup.get(cited)
            if not cited_paper_id:
                cited_paper_id = self.add_citation_stub(cited)
            self.cites.add((paper_id, cited_paper_id))

    def load_proceedings_metadata(self, rows: List[Dict[str, str]]) -> None:
        for row in rows:
            key = (row.get("key") or "").strip()
            if key.startswith("dblpnote/"):
                continue

            year = safe_int(row.get("year"), None)
            if year is None:
                continue

            venue_name = (row.get("booktitle") or row.get("title") or "").strip()
            if not venue_name:
                continue

            isbn_values = split_multi(row.get("isbn", ""))
            isbn = isbn_values[0] if isbn_values else "unknown"

            self.proc_meta_by_key[(venue_name.lower(), year)] = {
                "city": (row.get("address") or "Unknown City").strip() or "Unknown City",
                "isbn": isbn,
                "proceeding_title": (row.get("title") or f"Proceedings of {venue_name} {year}").strip(),
            }

    def synthesize_reviewers(self) -> None:
        author_ids = sorted(self.all_author_ids)
        for paper_id in self.papers:
            own_authors = self.paper_authors.get(paper_id, set())
            candidates = [a for a in author_ids if a not in own_authors]
            if not candidates:
                continue
            k = min(self.reviewers_per_paper, len(candidates))
            for reviewer_id in self.random.sample(candidates, k=k):
                self.reviewed.add((reviewer_id, paper_id))

    def write_csv(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)

        self._write(out_dir, "authors.csv", ["authorId", "name"], self.authors.values())
        self._write(out_dir, "papers.csv", ["paperId", "doi", "title", "year", "abstract", "pages"], self.papers.values())
        self._write(out_dir, "keywords.csv", ["term"], self.keywords.values())
        self._write(out_dir, "journals.csv", ["journalId", "name", "issn"], self.journals.values())
        self._write(out_dir, "journal_volumes.csv", ["journalVolumeId", "year", "volume"], self.journal_volumes.values())
        self._write(out_dir, "proceedings.csv", ["proceedingId", "title", "isbn"], self.proceedings.values())
        self._write(out_dir, "editions.csv", ["editionId", "year", "city"], self.editions.values())
        self._write(out_dir, "venues.csv", ["venueId", "name"], self.venues.values())

        self._write_tuples(out_dir, "authored.csv", ["authorId", "paperId", "position"], sorted(self.authored))
        self._write_tuples(out_dir, "corresponding_author.csv", ["authorId", "paperId"], sorted(self.corresponding_author))
        self._write_tuples(out_dir, "reviewed.csv", ["authorId", "paperId"], sorted(self.reviewed))
        self._write_tuples(out_dir, "cites.csv", ["citingPaperId", "citedPaperId"], sorted(self.cites))
        self._write_tuples(out_dir, "has_keyword.csv", ["paperId", "term"], sorted(self.has_keyword))
        self._write_tuples(out_dir, "published_in_volume.csv", ["paperId", "journalVolumeId"], sorted(self.published_in_volume))
        self._write_tuples(out_dir, "published_in_proceedings.csv", ["paperId", "proceedingId"], sorted(self.published_in_proceedings))
        self._write_tuples(out_dir, "volume_of.csv", ["journalVolumeId", "journalId"], sorted(self.volume_of))
        self._write_tuples(out_dir, "for_edition.csv", ["proceedingId", "editionId"], sorted(self.for_edition))
        self._write_tuples(out_dir, "edition_of.csv", ["editionId", "venueId"], sorted(self.edition_of))

    @staticmethod
    def _write(out_dir: str, filename: str, headers: List[str], rows: Iterable[Dict[str, Any]]) -> None:
        with open(os.path.join(out_dir, filename), "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow({h: row.get(h, "") for h in headers})

    @staticmethod
    def _write_tuples(out_dir: str, filename: str, headers: List[str], rows: Iterable[Tuple[Any, ...]]) -> None:
        with open(os.path.join(out_dir, filename), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)


def write_load_cypher(out_dir: str) -> None:
    query = r'''
CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE;
CREATE CONSTRAINT paper_id IF NOT EXISTS FOR (p:Paper) REQUIRE p.paperId IS UNIQUE;
CREATE CONSTRAINT keyword_term IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE;
CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE;
CREATE CONSTRAINT journal_volume_id IF NOT EXISTS FOR (jv:JournalVolume) REQUIRE jv.journalVolumeId IS UNIQUE;
CREATE CONSTRAINT proceeding_id IF NOT EXISTS FOR (pr:Proceedings) REQUIRE pr.proceedingId IS UNIQUE;
CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (e:Edition) REQUIRE e.editionId IS UNIQUE;
CREATE CONSTRAINT venue_id IF NOT EXISTS FOR (v:ConferenceWorkshop) REQUIRE v.venueId IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row
MERGE (a:Author {authorId: row.authorId})
SET a.name = row.name;

LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MERGE (p:Paper {paperId: row.paperId})
SET p.DOI = row.doi,
    p.title = row.title,
    p.year = toInteger(row.year),
    p.abstract = row.abstract,
    p.pages = row.pages;

LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' AS row
MERGE (:Keyword {term: row.term});

LOAD CSV WITH HEADERS FROM 'file:///journals.csv' AS row
MERGE (j:Journal {journalId: row.journalId})
SET j.name = row.name,
    j.issn = row.issn;

LOAD CSV WITH HEADERS FROM 'file:///journal_volumes.csv' AS row
MERGE (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
SET jv.year = toInteger(row.year),
    jv.volume = row.volume;

LOAD CSV WITH HEADERS FROM 'file:///proceedings.csv' AS row
MERGE (pr:Proceedings {proceedingId: row.proceedingId})
SET pr.title = row.title,
    pr.isbn = row.isbn;

LOAD CSV WITH HEADERS FROM 'file:///editions.csv' AS row
MERGE (e:Edition {editionId: row.editionId})
SET e.year = toInteger(row.year),
    e.city = row.city;

LOAD CSV WITH HEADERS FROM 'file:///venues.csv' AS row
MERGE (v:ConferenceWorkshop {venueId: row.venueId})
SET v.name = row.name;

LOAD CSV WITH HEADERS FROM 'file:///authored.csv' AS row
MATCH (a:Author {authorId: row.authorId})
MATCH (p:Paper {paperId: row.paperId})
MERGE (a)-[r:AUTHORED]->(p)
SET r.position = toInteger(row.position);

LOAD CSV WITH HEADERS FROM 'file:///corresponding_author.csv' AS row
MATCH (a:Author {authorId: row.authorId})
MATCH (p:Paper {paperId: row.paperId})
MERGE (a)-[:CORRESPONDING_AUTHOR]->(p);

LOAD CSV WITH HEADERS FROM 'file:///reviewed.csv' AS row
MATCH (a:Author {authorId: row.authorId})
MATCH (p:Paper {paperId: row.paperId})
MERGE (a)-[:REVIEWED]->(p);

LOAD CSV WITH HEADERS FROM 'file:///cites.csv' AS row
MATCH (p1:Paper {paperId: row.citingPaperId})
MATCH (p2:Paper {paperId: row.citedPaperId})
MERGE (p1)-[:CITES]->(p2);

LOAD CSV WITH HEADERS FROM 'file:///has_keyword.csv' AS row
MATCH (p:Paper {paperId: row.paperId})
MATCH (k:Keyword {term: row.term})
MERGE (p)-[:HAS_KEYWORD]->(k);

LOAD CSV WITH HEADERS FROM 'file:///published_in_volume.csv' AS row
MATCH (p:Paper {paperId: row.paperId})
MATCH (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
MERGE (p)-[:PUBLISHED_IN]->(jv);

LOAD CSV WITH HEADERS FROM 'file:///published_in_proceedings.csv' AS row
MATCH (p:Paper {paperId: row.paperId})
MATCH (pr:Proceedings {proceedingId: row.proceedingId})
MERGE (p)-[:PUBLISHED_IN]->(pr);

LOAD CSV WITH HEADERS FROM 'file:///volume_of.csv' AS row
MATCH (jv:JournalVolume {journalVolumeId: row.journalVolumeId})
MATCH (j:Journal {journalId: row.journalId})
MERGE (jv)-[:VOLUME_OF]->(j);

LOAD CSV WITH HEADERS FROM 'file:///for_edition.csv' AS row
MATCH (pr:Proceedings {proceedingId: row.proceedingId})
MATCH (e:Edition {editionId: row.editionId})
MERGE (pr)-[:FOR_EDITION]->(e);

LOAD CSV WITH HEADERS FROM 'file:///edition_of.csv' AS row
MATCH (e:Edition {editionId: row.editionId})
MATCH (v:ConferenceWorkshop {venueId: row.venueId})
MERGE (e)-[:EDITION_OF]->(v);
'''.strip() + "\n"

    with open(os.path.join(out_dir, "load_a2.cypher"), "w", encoding="utf-8") as f:
        f.write(query)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transform DBLP intermediate files plus *_header files into final CSVs for the graph model."
    )
    parser.add_argument("--article", required=True)
    parser.add_argument("--article-header", required=True)
    parser.add_argument("--inproceedings", required=True)
    parser.add_argument("--inproceedings-header", required=True)
    parser.add_argument("--proceedings")
    parser.add_argument("--proceedings-header")
    parser.add_argument("--out", default="neo4j_import")
    parser.add_argument("--reviewers-per-paper", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    builder = GraphBuilder(reviewers_per_paper=args.reviewers_per_paper, seed=args.seed)

    if args.proceedings and args.proceedings_header:
        proceedings_rows = read_intermediate_with_header(
            args.proceedings, args.proceedings_header, limit=args.limit
        )
        builder.load_proceedings_metadata(proceedings_rows)

    article_rows = read_intermediate_with_header(
        args.article, args.article_header, limit=args.limit
    )
    for row in article_rows:
        builder.process_article_row(row)

    inproceedings_rows = read_intermediate_with_header(
        args.inproceedings, args.inproceedings_header, limit=args.limit
    )
    for row in inproceedings_rows:
        builder.process_inproceedings_row(row)

    builder.synthesize_reviewers()
    builder.write_csv(args.out)
    write_load_cypher(args.out)

    print(f"Generated final CSV files in: {args.out}")
    print("Copy the CSV files to Neo4j's import/ folder and run load_a2.cypher from Neo4j Browser.")


if __name__ == "__main__":
    main()