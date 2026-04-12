#!/usr/bin/env python3
import argparse
import csv
import os
import random
import re
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# Summary:
# FormatCSV reads the intermediate DBLP CSV files, selects a curated subset of
# articles and inproceedings, and transforms them into the node and relationship
# CSVs of the A.2 graph model. During the process it infers keywords, creates
# synthetic missing metadata, reinforces internal citations, assigns reviewers
# and writes the final CSV dataset to data/csv_graphmodel_A2_data/.

KEYWORD_FALLBACKS = {
    "graph": ["graph processing", "property graph"],
    "database": ["data management", "data querying"],
    "query": ["data querying"],
    "index": ["indexing"],
    "model": ["data modeling"],
    "big data": ["big data"],
    "storage": ["data storage"],
    "process": ["data processing"],
    "data": ["data management"],
    "mining": ["data processing"],
    "retrieval": ["data querying"],
    "knowledge": ["data modeling"],
}

DATABASE_TERMS = {
    "data management",
    "indexing",
    "data modeling",
    "big data",
    "data processing",
    "data storage",
    "data querying",
    "graph processing",
    "property graph",
}


def log_step(message: str) -> None:
    print(f"[INFO] {message}", flush=True)


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


def nonempty_citations(row: Dict[str, str]) -> List[str]:
    cites = []
    for cited in split_multi(row.get("cite", "")):
        low = cited.lower()
        if cited and cited != "..." and low != "omitted":
            cites.append(cited)
    return cites


def infer_keywords_from_title(title: str) -> List[str]:
    found = set()
    low = (title or "").lower()
    for trigger, kws in KEYWORD_FALLBACKS.items():
        if trigger in low:
            found.update(kws)
    if not found:
        found.add("general research")
    return sorted(found)


def row_authors(row: Dict[str, str]) -> List[str]:
    return split_multi(row.get("author", ""))


def normalized_venue_name(value: str) -> str:
    value = (value or "").strip()
    value = re.sub(r"\s+", " ", value)
    return value


def row_container(row: Dict[str, str], kind: str) -> str:
    if kind == "article":
        return normalized_venue_name(row.get("journal", ""))
    return normalized_venue_name(row.get("booktitle", ""))


def row_year(row: Dict[str, str]) -> int:
    return safe_int(row.get("year"), 2000) or 2000


def row_title(row: Dict[str, str]) -> str:
    return (row.get("title") or "").strip()


def row_key(row: Dict[str, str]) -> str:
    return (row.get("key") or "").strip()


def is_valid_row(row: Dict[str, str], kind: str) -> bool:
    key = row_key(row)
    if key.startswith("dblpnote/"):
        return False
    title = row_title(row)
    container = row_container(row, kind)
    return bool(title and container)


def compute_row_stats(rows: List[Dict[str, str]], kind: str) -> Dict[str, Any]:
    author_freq = Counter()
    container_freq = Counter()
    container_years = defaultdict(set)

    for row in rows:
        if not is_valid_row(row, kind):
            continue

        container = row_container(row, kind)
        year = row_year(row)
        authors = row_authors(row)

        if container:
            container_freq[container] += 1
            container_years[container].add(year)
        for a in authors:
            author_freq[a] += 1

    return {
        "author_freq": author_freq,
        "container_freq": container_freq,
        "container_years": container_years,
    }


def score_row(row: Dict[str, str], kind: str, stats: Dict[str, Any]) -> float:
    title = row_title(row)
    container = row_container(row, kind)
    authors = row_authors(row)
    citations = nonempty_citations(row)
    keywords = infer_keywords_from_title(title)

    author_freq: Counter = stats["author_freq"]
    container_freq: Counter = stats["container_freq"]
    container_years: Dict[str, Set[int]] = stats["container_years"]

    repeated_authors = sum(1 for a in authors if author_freq[a] >= 2)
    very_repeated_authors = sum(1 for a in authors if author_freq[a] >= 3)
    year_span = len(container_years.get(container, set()))
    db_bonus = sum(1 for kw in keywords if kw in DATABASE_TERMS)

    score = 0.0
    score += min(len(citations), 8) * 5.0
    score += min(len(authors), 4) * 2.0
    score += repeated_authors * 3.0
    score += very_repeated_authors * 2.0
    score += min(container_freq.get(container, 0), 20) * 1.3
    score += min(year_span, 8) * 2.5
    score += db_bonus * 1.5

    if kind == "inproceedings":
        score += 2.0
    if len(citations) > 0:
        score += 8.0
    if year_span >= 4:
        score += 10.0

    return score


def select_curated_rows(
    rows: List[Dict[str, str]],
    kind: str,
    target_count: int,
    preferred_containers: Optional[Set[str]] = None,
) -> List[Dict[str, str]]:
    candidates = [r for r in rows if is_valid_row(r, kind)]
    stats = compute_row_stats(candidates, kind)

    ranked: List[Tuple[float, Dict[str, str]]] = []
    for row in candidates:
        base_score = score_row(row, kind, stats)
        container = row_container(row, kind)
        if preferred_containers and container in preferred_containers:
            base_score += 12.0
        ranked.append((base_score, row))

    ranked.sort(key=lambda x: x[0], reverse=True)

    selected: List[Dict[str, str]] = []
    used_keys: Set[str] = set()
    container_quota = defaultdict(int)
    yearly_quota = defaultdict(int)

    for _, row in ranked:
        if len(selected) >= target_count:
            break

        key = row_key(row)
        if key and key in used_keys:
            continue

        container = row_container(row, kind)
        year = row_year(row)

        if container_quota[container] >= max(8, target_count // 4):
            continue
        if yearly_quota[(container, year)] >= 6:
            continue

        selected.append(row)
        if key:
            used_keys.add(key)
        container_quota[container] += 1
        yearly_quota[(container, year)] += 1

    return selected


class GraphBuilder:
    SYNTHETIC_CITIES = [
        "Barcelona",
        "Madrid",
        "Paris",
        "Berlin",
        "Rome",
        "Lisbon",
        "Amsterdam",
        "Vienna",
        "Prague",
        "Dublin",
        "Zurich",
        "Copenhagen",
        "Helsinki",
        "Warsaw",
        "Brussels",
    ]

    SYNTHETIC_PAGES = [
        "1-10",
        "11-20",
        "21-30",
        "31-40",
        "41-50",
        "51-60",
        "61-70",
        "71-80",
        "81-90",
        "91-100",
        "101-110",
        "111-120",
    ]

    ABSTRACT_TEMPLATES = [
        "This paper studies {topic} using a graph-based approach.",
        "This work presents a synthetic research summary for {topic}.",
        "This article explores methods and challenges related to {topic}.",
        "This contribution investigates {topic} with an experimental methodology.",
        "This paper analyzes {topic} from a data management perspective.",
        "This work evaluates techniques and applications related to {topic}.",
    ]

    def __init__(
        self,
        reviewers_per_paper: int = 3,
        seed: int = 42,
        min_internal_cites_per_paper: int = 2,
        max_internal_cites_per_paper: int = 4,
    ) -> None:
        self.random = random.Random(seed)
        self.reviewers_per_paper = reviewers_per_paper
        self.min_internal_cites_per_paper = min_internal_cites_per_paper
        self.max_internal_cites_per_paper = max_internal_cites_per_paper

        self.authors: Dict[str, Dict[str, Any]] = {}
        self.papers: Dict[str, Dict[str, Any]] = {}
        self.keywords: Dict[str, Dict[str, Any]] = {}
        self.journals: Dict[str, Dict[str, Any]] = {}
        self.journal_volumes: Dict[str, Dict[str, Any]] = {}
        self.editions: Dict[str, Dict[str, Any]] = {}
        self.venues: Dict[str, Dict[str, Any]] = {}

        self.authored: Set[Tuple[str, str, int]] = set()
        self.corresponding_author: Set[Tuple[str, str]] = set()
        self.reviewed: Set[Tuple[str, str]] = set()
        self.cites: Set[Tuple[str, str]] = set()
        self.has_keyword: Set[Tuple[str, str]] = set()
        self.published_in_volume: Set[Tuple[str, str]] = set()
        self.published_in_edition: Set[Tuple[str, str]] = set()
        self.volume_of: Set[Tuple[str, str]] = set()
        self.edition_of: Set[Tuple[str, str]] = set()

        self.paper_authors: Dict[str, Set[str]] = defaultdict(set)
        self.all_author_ids: Set[str] = set()
        self.paper_lookup: Dict[str, str] = {}

    def infer_keywords(self, title: str) -> List[str]:
        return infer_keywords_from_title(title)

    def synthetic_city(self) -> str:
        return self.random.choice(self.SYNTHETIC_CITIES)

    def synthetic_pages(self) -> str:
        return self.random.choice(self.SYNTHETIC_PAGES)

    def synthetic_issn(self) -> str:
        first = self.random.randint(1000, 9999)
        second = self.random.randint(1000, 9999)
        return f"{first}-{second}"

    def synthetic_abstract(self, title: str) -> str:
        topic = (title or "this topic").strip().rstrip(".").lower()
        template = self.random.choice(self.ABSTRACT_TEMPLATES)
        return template.format(topic=topic)

    def add_author(self, author_name: str) -> str:
        author_id = f"author_{slug(author_name)}"
        self.authors.setdefault(author_id, {"authorId": author_id, "name": author_name})
        self.all_author_ids.add(author_id)
        return author_id

    def add_keyword(self, term: str) -> None:
        term = (term or "").strip().lower()
        if term:
            self.keywords.setdefault(term, {"term": term})

    def make_paper_doi(self, row: Dict[str, str], fallback_prefix: str) -> str:
        doi = guess_doi(row.get("ee", ""))
        if doi:
            return doi

        key = row_key(row)
        title = row_title(row) or "untitled"
        year = row_year(row)
        if key:
            return f"10.synthetic/{slug(key)}"
        return f"10.synthetic/{slug(fallback_prefix + '-' + title + '-' + str(year))}"

    def add_citation_stub(self, raw_cite: str) -> str:
        cited_doi = raw_cite if re.match(r"^10\.\S+", raw_cite) else f"10.synthetic/cite-{slug(raw_cite)}"
        self.papers.setdefault(
            cited_doi,
            {
                "DOI": cited_doi,
                "title": raw_cite,
                "year": 2000,
                "abstract": self.synthetic_abstract(raw_cite),
                "pages": self.synthetic_pages(),
            },
        )
        self.add_keyword("general research")
        self.has_keyword.add((cited_doi, "general research"))
        return cited_doi

    def find_existing_venue_id(self, candidate_name: str) -> Optional[str]:
        cand_slug = slug(candidate_name)
        direct_id = f"venue_{cand_slug}"
        if direct_id in self.venues:
            return direct_id
        for venue_id, venue in self.venues.items():
            existing_slug = slug(venue.get("name", ""))
            if existing_slug == cand_slug:
                return venue_id
        return None

    def find_existing_edition_id(self, venue_id: str, year: int) -> Optional[str]:
        for edition_id, linked_venue_id in self.edition_of:
            if linked_venue_id == venue_id:
                edition = self.editions.get(edition_id)
                if edition and edition.get("year") == year:
                    return edition_id
        return None

    def create_or_get_edition(self, venue_name: str, year: int, city: str, proceedings_title: str) -> Tuple[str, str]:
        venue_id = self.find_existing_venue_id(venue_name)
        if venue_id is None:
            venue_id = f"venue_{slug(venue_name)}"
            self.venues.setdefault(venue_id, {"venueId": venue_id, "name": venue_name})

        edition_id = self.find_existing_edition_id(venue_id, year)
        if edition_id is None:
            edition_id = f"edition_{slug(self.venues[venue_id]['name'])}_{year}"
            self.editions.setdefault(
                edition_id,
                {
                    "editionId": edition_id,
                    "year": year,
                    "city": city,
                    "proceedingsTitle": proceedings_title,
                },
            )
            self.edition_of.add((edition_id, venue_id))
        else:
            edition = self.editions[edition_id]
            if city and not edition.get("city"):
                edition["city"] = city
            if proceedings_title and not edition.get("proceedingsTitle"):
                edition["proceedingsTitle"] = proceedings_title

        return venue_id, edition_id

    def process_article_row(self, row: Dict[str, str]) -> None:
        title = row_title(row)
        journal_name = row_container(row, "article")
        if not title or not journal_name:
            return

        paper_doi = self.make_paper_doi(row, "article")
        year = row_year(row)
        pages = (row.get("pages") or "").strip() or self.synthetic_pages()
        volume = str(row.get("volume") or "1").strip()

        self.papers.setdefault(
            paper_doi,
            {
                "DOI": paper_doi,
                "title": title,
                "year": year,
                "abstract": self.synthetic_abstract(title),
                "pages": pages,
            },
        )

        key = row_key(row)
        if key:
            self.paper_lookup[key] = paper_doi

        authors = row_authors(row)
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_doi, pos))
            self.paper_authors[paper_doi].add(author_id)

        if authors:
            self.corresponding_author.add((paper_doi, f"author_{slug(authors[0])}"))

        keywords = self.infer_keywords(title)
        for kw in keywords:
            self.add_keyword(kw)
            self.has_keyword.add((paper_doi, kw))

        journal_id = f"journal_{slug(journal_name)}"
        jv_id = f"jv_{slug(journal_name)}_{year}_{slug(volume)}"

        self.journals.setdefault(
            journal_id,
            {"journalId": journal_id, "name": journal_name, "issn": self.synthetic_issn()},
        )
        self.journal_volumes.setdefault(
            jv_id,
            {"journalVolumeId": jv_id, "year": year, "volumeNumber": volume},
        )

        self.volume_of.add((jv_id, journal_id))
        self.published_in_volume.add((paper_doi, jv_id))

        for cited in nonempty_citations(row):
            cited_paper_doi = self.paper_lookup.get(cited)
            if not cited_paper_doi:
                cited_paper_doi = self.add_citation_stub(cited)
            if cited_paper_doi != paper_doi:
                self.cites.add((paper_doi, cited_paper_doi))

    def process_inproceedings_row(self, row: Dict[str, str]) -> None:
        title = row_title(row)
        venue_name = row_container(row, "inproceedings")
        if not title or not venue_name:
            return

        paper_doi = self.make_paper_doi(row, "inproceedings")
        year = row_year(row)
        pages = (row.get("pages") or "").strip() or self.synthetic_pages()

        self.papers.setdefault(
            paper_doi,
            {
                "DOI": paper_doi,
                "title": title,
                "year": year,
                "abstract": self.synthetic_abstract(title),
                "pages": pages,
            },
        )

        key = row_key(row)
        if key:
            self.paper_lookup[key] = paper_doi

        authors = row_authors(row)
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_doi, pos))
            self.paper_authors[paper_doi].add(author_id)

        if authors:
            self.corresponding_author.add((paper_doi, f"author_{slug(authors[0])}"))

        keywords = self.infer_keywords(title)
        for kw in keywords:
            self.add_keyword(kw)
            self.has_keyword.add((paper_doi, kw))

        city = (row.get("address") or "").strip() or self.synthetic_city()
        proceedings_title = f"Proceedings of {venue_name} {year}"

        _, edition_id = self.create_or_get_edition(
            venue_name=venue_name,
            year=year,
            city=city,
            proceedings_title=proceedings_title,
        )

        self.published_in_edition.add((paper_doi, edition_id))

        for cited in nonempty_citations(row):
            cited_paper_doi = self.paper_lookup.get(cited)
            if not cited_paper_doi:
                cited_paper_doi = self.add_citation_stub(cited)
            if cited_paper_doi != paper_doi:
                self.cites.add((paper_doi, cited_paper_doi))

    def process_proceedings_row(self, row: Dict[str, str]) -> None:
        year = row_year(row)
        raw_title = (row.get("title") or "").strip()
        raw_booktitle = (row.get("booktitle") or "").strip()
        raw_address = (row.get("address") or "").strip()

        venue_name = normalized_venue_name(raw_booktitle or raw_title)
        if not venue_name:
            return

        proceedings_title = raw_title or f"Proceedings of {venue_name} {year}"
        city = raw_address.strip() or self.synthetic_city()
        self.create_or_get_edition(venue_name, year, city, proceedings_title)

    def synthesize_reviewers(self) -> None:
        author_ids = sorted(self.all_author_ids)
        papers = list(self.papers.keys())
        total = len(papers)

        for idx, paper_id in enumerate(papers, start=1):
            own_authors = self.paper_authors.get(paper_id, set())
            candidates = [a for a in author_ids if a not in own_authors]
            if candidates:
                k = min(self.reviewers_per_paper, len(candidates))
                for reviewer_id in self.random.sample(candidates, k=k):
                    self.reviewed.add((reviewer_id, paper_id))

            if idx % 25 == 0 or idx == total:
                log_step(f"Reviewer generation progress: {idx}/{total}")

    def _paper_keywords_map(self) -> Dict[str, Set[str]]:
        paper_kw = defaultdict(set)
        for doi, term in self.has_keyword:
            paper_kw[doi].add(term)
        return paper_kw

    def _internal_cite_candidates(self, citing_doi: str) -> List[str]:
        paper_kw = self._paper_keywords_map()
        citing_paper = self.papers[citing_doi]
        citing_year = safe_int(citing_paper.get("year"), 2000) or 2000
        citing_authors = self.paper_authors.get(citing_doi, set())
        citing_keywords = paper_kw.get(citing_doi, set())

        candidates = []
        for other_doi, other_paper in self.papers.items():
            if other_doi == citing_doi:
                continue
            other_year = safe_int(other_paper.get("year"), 2000) or 2000
            if other_year > citing_year:
                continue
            if (citing_doi, other_doi) in self.cites:
                continue

            other_authors = self.paper_authors.get(other_doi, set())
            if citing_authors & other_authors:
                continue

            other_keywords = paper_kw.get(other_doi, set())
            if citing_keywords & other_keywords:
                candidates.append(other_doi)

        self.random.shuffle(candidates)
        return candidates

    def strengthen_internal_citations(self) -> None:
        real_papers = [
            doi for doi in self.papers
            if doi in self.paper_authors and len(self.paper_authors[doi]) > 0
        ]
        total = len(real_papers)

        for idx, doi in enumerate(real_papers, start=1):
            current_out = sum(1 for src, _ in self.cites if src == doi)
            needed = self.min_internal_cites_per_paper - current_out
            candidates = self._internal_cite_candidates(doi)

            added = 0
            if needed > 0:
                for target in candidates:
                    if added >= needed:
                        break
                    self.cites.add((doi, target))
                    added += 1

            current_out = sum(1 for src, _ in self.cites if src == doi)
            extra_room = self.max_internal_cites_per_paper - current_out
            if extra_room > 0 and candidates:
                extra = min(extra_room, self.random.randint(0, 2))
                for target in candidates[added:added + extra]:
                    self.cites.add((doi, target))

            if idx % 25 == 0 or idx == total:
                log_step(f"Internal citation reinforcement progress: {idx}/{total}")

    def ensure_keyword_density(self) -> None:
        papers = list(self.papers.items())
        total = len(papers)

        for idx, (doi, paper) in enumerate(papers, start=1):
            kws = [term for paper_doi, term in self.has_keyword if paper_doi == doi]
            if len(kws) < 2:
                inferred = self.infer_keywords(str(paper.get("title", "")))
                for kw in inferred[:2]:
                    self.add_keyword(kw)
                    self.has_keyword.add((doi, kw))

            if idx % 50 == 0 or idx == total:
                log_step(f"Keyword density progress: {idx}/{total}")

    def write_csv(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)

        log_step("Writing authors.csv")
        self._write(out_dir, "authors.csv", ["authorId", "name"], self.authors.values())

        log_step("Writing papers.csv")
        self._write(out_dir, "papers.csv", ["DOI", "title", "year", "abstract", "pages"], self.papers.values())

        log_step("Writing keywords.csv")
        self._write(out_dir, "keywords.csv", ["term"], self.keywords.values())

        log_step("Writing journals.csv")
        self._write(out_dir, "journals.csv", ["journalId", "name", "issn"], self.journals.values())

        log_step("Writing journal_volumes.csv")
        self._write(out_dir, "journal_volumes.csv", ["journalVolumeId", "year", "volumeNumber"], self.journal_volumes.values())

        log_step("Writing editions.csv")
        self._write(out_dir, "editions.csv", ["editionId", "year", "city", "proceedingsTitle"], self.editions.values())

        log_step("Writing venues.csv")
        self._write(out_dir, "venues.csv", ["venueId", "name"], self.venues.values())

        log_step("Writing authored.csv")
        self._write_tuples(out_dir, "authored.csv", ["authorId", "DOI", "position"], sorted(self.authored))

        log_step("Writing corresponding_author.csv")
        self._write_tuples(out_dir, "corresponding_author.csv", ["DOI", "authorId"], sorted(self.corresponding_author))

        log_step("Writing reviewed.csv")
        self._write_tuples(out_dir, "reviewed.csv", ["authorId", "DOI"], sorted(self.reviewed))

        log_step("Writing cites.csv")
        self._write_tuples(out_dir, "cites.csv", ["citingDOI", "citedDOI"], sorted(self.cites))

        log_step("Writing has_keyword.csv")
        self._write_tuples(out_dir, "has_keyword.csv", ["DOI", "term"], sorted(self.has_keyword))

        log_step("Writing published_in_volume.csv")
        self._write_tuples(out_dir, "published_in_volume.csv", ["DOI", "journalVolumeId"], sorted(self.published_in_volume))

        log_step("Writing published_in_edition.csv")
        self._write_tuples(out_dir, "published_in_edition.csv", ["DOI", "editionId"], sorted(self.published_in_edition))

        log_step("Writing volume_of.csv")
        self._write_tuples(out_dir, "volume_of.csv", ["journalVolumeId", "journalId"], sorted(self.volume_of))

        log_step("Writing edition_of.csv")
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


def choose_preferred_conference_venues(rows: List[Dict[str, str]], top_k: int = 5) -> Set[str]:
    stats = compute_row_stats(rows, "inproceedings")
    container_freq: Counter = stats["container_freq"]
    container_years: Dict[str, Set[int]] = stats["container_years"]

    scored = []
    for venue, freq in container_freq.items():
        years = len(container_years.get(venue, set()))
        score = freq * 2 + years * 6
        scored.append((score, venue))

    scored.sort(reverse=True)
    return {venue for _, venue in scored[:top_k]}


def choose_preferred_journals(rows: List[Dict[str, str]], top_k: int = 3) -> Set[str]:
    stats = compute_row_stats(rows, "article")
    container_freq: Counter = stats["container_freq"]

    scored = sorted(((freq, journal) for journal, freq in container_freq.items()), reverse=True)
    return {journal for _, journal in scored[:top_k]}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a curated graph CSV dataset with fewer but better-connected instances."
    )
    parser.add_argument("--article", default="output_article.csv")
    parser.add_argument("--article-header", default="output_article_header.csv")
    parser.add_argument("--inproceedings", default="output_inproceedings.csv")
    parser.add_argument("--inproceedings-header", default="output_inproceedings_header.csv")
    parser.add_argument("--proceedings", default="output_proceedings.csv")
    parser.add_argument("--proceedings-header", default="output_proceedings_header.csv")

    parser.add_argument("--target-articles", type=int, default=35)
    parser.add_argument("--target-inproceedings", type=int, default=90)
    parser.add_argument("--scan-articles", type=int, default=None)
    parser.add_argument("--scan-inproceedings", type=int, default=None)
    parser.add_argument("--scan-proceedings", type=int, default=None)

    parser.add_argument("--reviewers-per-paper", type=int, default=3)
    parser.add_argument("--min-internal-cites", type=int, default=3)
    parser.add_argument("--max-internal-cites", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent

    input_dir = project_root / "data" / "csv_dblp_data"
    output_dir = project_root / "data" / "csv_graphmodel_A2_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    article_path = input_dir / args.article
    article_header_path = input_dir / args.article_header
    inproceedings_path = input_dir / args.inproceedings
    inproceedings_header_path = input_dir / args.inproceedings_header
    proceedings_path = input_dir / args.proceedings if args.proceedings else None
    proceedings_header_path = input_dir / args.proceedings_header if args.proceedings_header else None

    log_step(f"Input directory: {input_dir}")
    log_step(f"Output directory: {output_dir}")

    if not article_path.exists():
        raise FileNotFoundError(f"Missing file: {article_path}")
    if not article_header_path.exists():
        raise FileNotFoundError(f"Missing file: {article_header_path}")
    if not inproceedings_path.exists():
        raise FileNotFoundError(f"Missing file: {inproceedings_path}")
    if not inproceedings_header_path.exists():
        raise FileNotFoundError(f"Missing file: {inproceedings_header_path}")
    if proceedings_path and not proceedings_path.exists():
        raise FileNotFoundError(f"Missing file: {proceedings_path}")
    if proceedings_header_path and not proceedings_header_path.exists():
        raise FileNotFoundError(f"Missing file: {proceedings_header_path}")

    log_step("Initializing graph builder...")
    builder = GraphBuilder(
        reviewers_per_paper=args.reviewers_per_paper,
        seed=args.seed,
        min_internal_cites_per_paper=args.min_internal_cites,
        max_internal_cites_per_paper=args.max_internal_cites,
    )

    log_step("Reading article rows...")
    article_rows = read_intermediate_with_header(
        str(article_path), str(article_header_path), limit=args.scan_articles
    )
    log_step(f"Loaded {len(article_rows)} article rows")

    log_step("Reading inproceedings rows...")
    inproceedings_rows = read_intermediate_with_header(
        str(inproceedings_path), str(inproceedings_header_path), limit=args.scan_inproceedings
    )
    log_step(f"Loaded {len(inproceedings_rows)} inproceedings rows")

    log_step("Choosing preferred venues and journals...")
    preferred_venues = choose_preferred_conference_venues(inproceedings_rows, top_k=5)
    preferred_journals = choose_preferred_journals(article_rows, top_k=3)
    log_step(f"Preferred venues: {len(preferred_venues)} | Preferred journals: {len(preferred_journals)}")

    log_step("Selecting curated inproceedings...")
    selected_inproceedings = select_curated_rows(
        inproceedings_rows,
        kind="inproceedings",
        target_count=args.target_inproceedings,
        preferred_containers=preferred_venues,
    )
    log_step(f"Selected {len(selected_inproceedings)} curated inproceedings")

    log_step("Selecting curated articles...")
    selected_articles = select_curated_rows(
        article_rows,
        kind="article",
        target_count=args.target_articles,
        preferred_containers=preferred_journals,
    )
    log_step(f"Selected {len(selected_articles)} curated articles")

    log_step("Building graph from selected articles...")
    total_articles = len(selected_articles)
    for i, row in enumerate(selected_articles, start=1):
        builder.process_article_row(row)
        if i % 10 == 0 or i == total_articles:
            log_step(f"Processed articles: {i}/{total_articles}")

    log_step("Building graph from selected inproceedings...")
    total_inproceedings = len(selected_inproceedings)
    for i, row in enumerate(selected_inproceedings, start=1):
        builder.process_inproceedings_row(row)
        if i % 10 == 0 or i == total_inproceedings:
            log_step(f"Processed inproceedings: {i}/{total_inproceedings}")

    if proceedings_path and proceedings_header_path:
        log_step("Reading proceedings rows...")
        proceedings_rows = read_intermediate_with_header(
            str(proceedings_path), str(proceedings_header_path), limit=args.scan_proceedings
        )
        log_step(f"Loaded {len(proceedings_rows)} proceedings rows")

        selected_venues = {row_container(r, "inproceedings") for r in selected_inproceedings}
        selected_years_by_venue = defaultdict(set)
        for r in selected_inproceedings:
            selected_years_by_venue[row_container(r, "inproceedings")].add(row_year(r))

        matched = 0
        total_proceedings = len(proceedings_rows)
        for i, row in enumerate(proceedings_rows, start=1):
            venue = normalized_venue_name(row.get("booktitle", "") or row.get("title", ""))
            year = row_year(row)
            if venue in selected_venues and year in selected_years_by_venue[venue]:
                builder.process_proceedings_row(row)
                matched += 1

            if i % 200 == 0 or i == total_proceedings:
                log_step(f"Proceedings scan progress: {i}/{total_proceedings}")

        log_step(f"Matched proceedings rows: {matched}")

    log_step("Ensuring keyword density...")
    builder.ensure_keyword_density()

    log_step("Strengthening internal citations...")
    builder.strengthen_internal_citations()

    log_step("Synthesizing reviewers...")
    builder.synthesize_reviewers()

    log_step("Writing CSV output...")
    builder.write_csv(str(output_dir))

    log_step("Done")
    print(f"Generated curated graph CSV files in: {output_dir}")
    print(f"Selected articles: {len(selected_articles)}")
    print(f"Selected inproceedings: {len(selected_inproceedings)}")
    print(f"Papers total: {len(builder.papers)}")
    print(f"Authors total: {len(builder.authors)}")
    print(f"CITES total: {len(builder.cites)}")


if __name__ == "__main__":
    main()
