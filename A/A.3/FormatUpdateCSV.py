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
    SYNTHETIC_CITIES = [
        "Barcelona", "Madrid", "Paris", "Berlin", "Rome",
        "Lisbon", "Amsterdam", "Vienna", "Prague", "Dublin",
        "Zurich", "Copenhagen", "Helsinki", "Warsaw", "Brussels",
    ]

    SYNTHETIC_PAGES = [
        "1-10", "11-20", "21-30", "31-40", "41-50", "51-60",
        "61-70", "71-80", "81-90", "91-100", "101-110", "111-120",
    ]

    ABSTRACT_TEMPLATES = [
        "This paper studies {topic} using a graph-based approach.",
        "This work presents a synthetic research summary for {topic}.",
        "This article explores methods and challenges related to {topic}.",
        "This contribution investigates {topic} with an experimental methodology.",
        "This paper analyzes {topic} from a data management perspective.",
        "This work evaluates techniques and applications related to {topic}.",
    ]

    POSITIVE_REVIEW_TEMPLATES = [
        "The paper presents a relevant contribution and is clearly written.",
        "The work is technically sound and the evaluation is convincing.",
        "The contribution is interesting and suitable for publication.",
        "The paper addresses an important problem with a solid methodology.",
    ]

    NEGATIVE_REVIEW_TEMPLATES = [
        "The contribution is limited and the evaluation is not strong enough.",
        "The paper lacks sufficient experimental validation.",
        "The presentation is unclear and several claims are not well supported.",
        "The work needs substantial improvements before publication.",
    ]

    ORG_SUFFIXES = [
        ("University", "University"),
        ("Institute", "University"),
        ("Lab", "Company"),
        ("Research", "Company"),
        ("Systems", "Company"),
        ("Technologies", "Company"),
    ]

    def __init__(self, seed: int = 42) -> None:
        self.random = random.Random(seed)

        self.organizations: Dict[str, Dict[str, Any]] = {}
        self.authors: Dict[str, Dict[str, Any]] = {}
        self.papers: Dict[str, Dict[str, Any]] = {}
        self.keywords: Dict[str, Dict[str, Any]] = {}
        self.journals: Dict[str, Dict[str, Any]] = {}
        self.journal_volumes: Dict[str, Dict[str, Any]] = {}
        self.editions: Dict[str, Dict[str, Any]] = {}
        self.conference_workshops: Dict[str, Dict[str, Any]] = {}

        self.affiliated_with: Set[Tuple[str, str]] = set()
        self.authored: Set[Tuple[str, str, int]] = set()
        self.corresponding_author: Set[Tuple[str, str]] = set()
        self.reviewed: Set[Tuple[str, str, str, str]] = set()
        self.cites: Set[Tuple[str, str]] = set()
        self.has_keyword: Set[Tuple[str, str]] = set()
        self.published_in_volume: Set[Tuple[str, str]] = set()
        self.published_in_edition: Set[Tuple[str, str]] = set()
        self.volume_of: Set[Tuple[str, str]] = set()
        self.edition_of: Set[Tuple[str, str]] = set()

        self.paper_to_journal_volume: Dict[str, str] = {}
        self.paper_to_edition: Dict[str, str] = {}

        self.paper_authors: Dict[str, Set[str]] = defaultdict(set)
        self.all_author_ids: Set[str] = set()
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

    def synthetic_city(self) -> str:
        return self.random.choice(self.SYNTHETIC_CITIES)

    def synthetic_pages(self) -> str:
        return self.random.choice(self.SYNTHETIC_PAGES)

    def synthetic_issn(self) -> str:
        return f"{self.random.randint(1000, 9999)}-{self.random.randint(1000, 9999)}"

    def synthetic_abstract(self, title: str) -> str:
        topic = (title or "this topic").strip().rstrip(".").lower()
        return self.random.choice(self.ABSTRACT_TEMPLATES).format(topic=topic)

    def synthetic_reviewer_policy_number(self) -> int:
        if self.random.random() < 0.85:
            return 3
        return self.random.choice([2, 4, 5])

    def synthetic_org_for_author(self, author_name: str) -> Tuple[str, str]:
        base = slug(author_name).split("-")
        token = base[-1].capitalize() if base else "Research"
        suffix, org_type = self.random.choice(self.ORG_SUFFIXES)
        org_name = f"{token} {suffix}"
        return org_name, org_type

    def get_reviewer_policy_for_paper(self, paper_id: str) -> int:
        jv_id = self.paper_to_journal_volume.get(paper_id)
        if jv_id:
            for volume_id, journal_id in self.volume_of:
                if volume_id == jv_id:
                    journal = self.journals.get(journal_id)
                    if journal and journal.get("reviewerPolicyNumber") is not None:
                        return int(journal["reviewerPolicyNumber"])

        edition_id = self.paper_to_edition.get(paper_id)
        if edition_id:
            for ed_id, venue_id in self.edition_of:
                if ed_id == edition_id:
                    venue = self.conference_workshops.get(venue_id)
                    if venue and venue.get("reviewerPolicyNumber") is not None:
                        return int(venue["reviewerPolicyNumber"])

        return 3

    def add_organization(self, org_name: str, org_type: str) -> str:
        org_id = f"org_{slug(org_name)}"
        self.organizations.setdefault(
            org_id,
            {"orgId": org_id, "name": org_name, "type": org_type},
        )
        return org_id

    def add_author(self, author_name: str) -> str:
        author_id = f"author_{slug(author_name)}"
        self.authors.setdefault(author_id, {"authorId": author_id, "name": author_name})
        self.all_author_ids.add(author_id)

        org_name, org_type = self.synthetic_org_for_author(author_name)
        org_id = self.add_organization(org_name, org_type)
        self.affiliated_with.add((author_id, org_id))

        return author_id

    def add_keyword(self, term: str) -> None:
        term = (term or "").strip().lower()
        if term:
            self.keywords.setdefault(term, {"term": term})

    def make_paper_doi(self, row: Dict[str, str], fallback_prefix: str) -> str:
        doi = guess_doi(row.get("ee", ""))
        if doi:
            return doi

        key = (row.get("key") or "").strip()
        title = (row.get("title") or "untitled").strip()
        year = safe_int(row.get("year"), 2000)

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
        return cited_doi

    def normalize_venue_name(self, text: str) -> str:
        text = (text or "").strip()
        text = re.sub(r"\s+", " ", text)

        prefixes = [
            r"^proceedings of the\s+",
            r"^proceedings of\s+the\s+",
            r"^proceedings of\s+",
            r"^selected papers from\s+the\s+",
            r"^selected papers of\s+the\s+",
        ]

        low = text.lower()
        for pattern in prefixes:
            new_low = re.sub(pattern, "", low, flags=re.IGNORECASE).strip()
            if new_low != low:
                text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
                low = new_low

        text = re.sub(r"\b\d{4}\b", "", text).strip(" ,:-")
        text = re.sub(r"\s+", " ", text)
        return text

    def looks_like_venue_title(self, title: str) -> bool:
        low = (title or "").lower()
        markers = ["proceedings", "conference", "workshop", "symposium", "forum", "colloquium"]
        return any(m in low for m in markers)

    def find_existing_venue_id(self, candidate_name: str) -> Optional[str]:
        cand_slug = slug(self.normalize_venue_name(candidate_name))
        if not cand_slug or cand_slug == "unknown":
            return None

        direct_id = f"venue_{cand_slug}"
        if direct_id in self.conference_workshops:
            return direct_id

        for venue_id, venue in self.conference_workshops.items():
            existing_slug = slug(self.normalize_venue_name(venue.get("name", "")))
            if existing_slug == cand_slug:
                return venue_id
            if cand_slug in existing_slug or existing_slug in cand_slug:
                return venue_id

        return None

    def find_existing_edition_id(self, venue_id: str, year: int) -> Optional[str]:
        for edition_id, linked_venue_id in self.edition_of:
            if linked_venue_id == venue_id:
                edition = self.editions.get(edition_id)
                if edition and edition.get("year") == year:
                    return edition_id
        return None

    def create_or_get_edition(
        self, venue_name: str, year: int, city: str, proceedings_title: str
    ) -> Tuple[str, str]:
        venue_id = self.find_existing_venue_id(venue_name)
        if venue_id is None:
            venue_id = f"venue_{slug(venue_name)}"
            self.conference_workshops.setdefault(
                venue_id,
                {
                    "venueId": venue_id,
                    "name": venue_name,
                    "reviewerPolicyNumber": self.synthetic_reviewer_policy_number(),
                },
            )

        edition_id = self.find_existing_edition_id(venue_id, year)
        if edition_id is None:
            edition_id = f"edition_{slug(self.conference_workshops[venue_id]['name'])}_{year}"
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
            if city and edition.get("city") in ("", None):
                edition["city"] = city
            if proceedings_title and (
                not edition.get("proceedingsTitle")
                or str(edition.get("proceedingsTitle", "")).startswith("Proceedings of ")
            ):
                edition["proceedingsTitle"] = proceedings_title

        return venue_id, edition_id

    def process_article_row(self, row: Dict[str, str]) -> None:
        key = (row.get("key") or "").strip()
        if key.startswith("dblpnote/"):
            return

        title = (row.get("title") or "").strip()
        journal_name = (row.get("journal") or "").strip()

        if not title or not journal_name:
            return

        paper_doi = self.make_paper_doi(row, "article")
        year = safe_int(row.get("year"), 2000)
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

        if key:
            self.paper_lookup[key] = paper_doi

        authors = split_multi(row.get("author", ""))
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_doi, pos))
            self.paper_authors[paper_doi].add(author_id)

        if authors:
            self.corresponding_author.add((paper_doi, f"author_{slug(authors[0])}"))

        for kw in self.infer_keywords(title):
            self.add_keyword(kw)
            self.has_keyword.add((paper_doi, kw))

        journal_id = f"journal_{slug(journal_name)}"
        jv_id = f"jv_{slug(journal_name)}_{year}_{slug(volume)}"

        self.journals.setdefault(
            journal_id,
            {
                "journalId": journal_id,
                "name": journal_name,
                "issn": self.synthetic_issn(),
                "reviewerPolicyNumber": self.synthetic_reviewer_policy_number(),
            },
        )
        self.journal_volumes.setdefault(
            jv_id,
            {
                "journalVolumeId": jv_id,
                "year": year,
                "volumeNumber": volume,
            },
        )

        self.volume_of.add((jv_id, journal_id))
        self.published_in_volume.add((paper_doi, jv_id))
        self.paper_to_journal_volume[paper_doi] = jv_id

        for cited in split_multi(row.get("cite", "")):
            if cited == "..." or cited.lower() == "omitted":
                continue
            cited_paper_doi = self.paper_lookup.get(cited)
            if not cited_paper_doi:
                cited_paper_doi = self.add_citation_stub(cited)
            self.cites.add((paper_doi, cited_paper_doi))

    def process_inproceedings_row(self, row: Dict[str, str]) -> None:
        key = (row.get("key") or "").strip()
        if key.startswith("dblpnote/"):
            return

        title = (row.get("title") or "").strip()
        venue_name = (row.get("booktitle") or "").strip()

        if not title or not venue_name:
            return

        paper_doi = self.make_paper_doi(row, "inproceedings")
        year = safe_int(row.get("year"), 2000)
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

        if key:
            self.paper_lookup[key] = paper_doi

        authors = split_multi(row.get("author", ""))
        for pos, author_name in enumerate(authors, start=1):
            author_id = self.add_author(author_name)
            self.authored.add((author_id, paper_doi, pos))
            self.paper_authors[paper_doi].add(author_id)

        if authors:
            self.corresponding_author.add((paper_doi, f"author_{slug(authors[0])}"))

        for kw in self.infer_keywords(title):
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
        self.paper_to_edition[paper_doi] = edition_id

        for cited in split_multi(row.get("cite", "")):
            if cited == "..." or cited.lower() == "omitted":
                continue
            cited_paper_doi = self.paper_lookup.get(cited)
            if not cited_paper_doi:
                cited_paper_doi = self.add_citation_stub(cited)
            self.cites.add((paper_doi, cited_paper_doi))

    def process_proceedings_row(self, row: Dict[str, str]) -> None:
        key = (row.get("key") or "").strip()
        if key.startswith("dblpnote/"):
            return

        year = safe_int(row.get("year"), None)
        if year is None:
            return

        raw_title = (row.get("title") or "").strip()
        raw_booktitle = (row.get("booktitle") or "").strip()
        raw_address = (row.get("address") or "").strip()

        if not raw_title and not raw_booktitle:
            return

        venue_name = raw_booktitle.strip()
        if not venue_name:
            if not self.looks_like_venue_title(raw_title):
                return
            venue_name = self.normalize_venue_name(raw_title)

        if not venue_name:
            return

        proceedings_title = raw_title or f"Proceedings of {venue_name} {year}"
        city = raw_address.strip()

        venue_id = self.find_existing_venue_id(venue_name)
        if venue_id is None:
            venue_id = f"venue_{slug(venue_name)}"
            self.conference_workshops.setdefault(
                venue_id,
                {
                    "venueId": venue_id,
                    "name": venue_name,
                    "reviewerPolicyNumber": self.synthetic_reviewer_policy_number(),
                },
            )

        edition_id = self.find_existing_edition_id(venue_id, year)
        if edition_id is None:
            edition_id = f"edition_{slug(self.conference_workshops[venue_id]['name'])}_{year}"
            self.editions.setdefault(
                edition_id,
                {
                    "editionId": edition_id,
                    "year": year,
                    "city": city or self.synthetic_city(),
                    "proceedingsTitle": proceedings_title,
                },
            )
            self.edition_of.add((edition_id, venue_id))
            return

        edition = self.editions[edition_id]

        current_city = (edition.get("city") or "").strip()
        if city and (not current_city or current_city in self.SYNTHETIC_CITIES):
            edition["city"] = city

        current_title = (edition.get("proceedingsTitle") or "").strip()
        if proceedings_title and (
            not current_title
            or current_title.startswith("Proceedings of ")
            or current_title == f"Proceedings of {self.conference_workshops[venue_id]['name']} {year}"
        ):
            edition["proceedingsTitle"] = proceedings_title

    def synthesize_reviewers(self) -> None:
        author_ids = sorted(self.all_author_ids)

        for paper_id in self.papers:
            own_authors = self.paper_authors.get(paper_id, set())
            candidates = [a for a in author_ids if a not in own_authors]
            if not candidates:
                continue

            required_reviewers = self.get_reviewer_policy_for_paper(paper_id)
            k = min(required_reviewers, len(candidates))
            if k == 0:
                continue

            selected_reviewers = self.random.sample(candidates, k=k)
            majority = (k // 2) + 1

            paper_should_be_accepted = self.random.random() < 0.7
            if paper_should_be_accepted:
                accept_count = self.random.randint(majority, k)
            else:
                accept_count = self.random.randint(0, k - majority)

            decisions = ["accept"] * accept_count + ["reject"] * (k - accept_count)
            self.random.shuffle(decisions)

            for reviewer_id, decision in zip(selected_reviewers, decisions):
                if decision == "accept":
                    content = self.random.choice(self.POSITIVE_REVIEW_TEMPLATES)
                else:
                    content = self.random.choice(self.NEGATIVE_REVIEW_TEMPLATES)

                self.reviewed.add((reviewer_id, paper_id, content, decision))

    def write_csv(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)

        self._write(out_dir, "organizations.csv", ["orgId", "name", "type"], self.organizations.values())
        self._write(out_dir, "authors.csv", ["authorId", "name"], self.authors.values())
        self._write(out_dir, "papers.csv", ["DOI", "title", "year", "abstract", "pages"], self.papers.values())
        self._write(out_dir, "keywords.csv", ["term"], self.keywords.values())

        self._write(
            out_dir,
            "journals.csv",
            ["journalId", "name", "issn", "reviewerPolicyNumber"],
            self.journals.values(),
        )
        self._write(
            out_dir,
            "journal_volumes.csv",
            ["journalVolumeId", "year", "volumeNumber"],
            self.journal_volumes.values(),
        )
        self._write(
            out_dir,
            "editions.csv",
            ["editionId", "year", "city", "proceedingsTitle"],
            self.editions.values(),
        )
        self._write(
            out_dir,
            "conference_workshops.csv",
            ["venueId", "name", "reviewerPolicyNumber"],
            self.conference_workshops.values(),
        )

        self._write_tuples(out_dir, "affiliated_with.csv", ["authorId", "orgId"], sorted(self.affiliated_with))
        self._write_tuples(out_dir, "authored.csv", ["authorId", "DOI", "position"], sorted(self.authored))
        self._write_tuples(out_dir, "corresponding_author.csv", ["DOI", "authorId"], sorted(self.corresponding_author))
        self._write_tuples(out_dir, "reviewed.csv", ["authorId", "DOI", "content", "suggestedDecision"], sorted(self.reviewed))
        self._write_tuples(out_dir, "cites.csv", ["citingDOI", "citedDOI"], sorted(self.cites))
        self._write_tuples(out_dir, "has_keyword.csv", ["DOI", "term"], sorted(self.has_keyword))
        self._write_tuples(out_dir, "published_in_volume.csv", ["DOI", "journalVolumeId"], sorted(self.published_in_volume))
        self._write_tuples(out_dir, "published_in_edition.csv", ["DOI", "editionId"], sorted(self.published_in_edition))
        self._write_tuples(out_dir, "volume_of.csv", ["journalVolumeId", "journalId"], sorted(self.volume_of))
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate final graph CSV files from DBLP intermediate files plus *_header files."
    )
    parser.add_argument("--article", required=True)
    parser.add_argument("--article-header", required=True)
    parser.add_argument("--inproceedings", required=True)
    parser.add_argument("--inproceedings-header", required=True)
    parser.add_argument("--proceedings")
    parser.add_argument("--proceedings-header")
    parser.add_argument("--out", default="neo4j_import")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    builder = GraphBuilder(seed=args.seed)

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

    if args.proceedings and args.proceedings_header:
        proceedings_rows = read_intermediate_with_header(
            args.proceedings, args.proceedings_header, limit=args.limit
        )
        for row in proceedings_rows:
            builder.process_proceedings_row(row)

    builder.synthesize_reviewers()
    builder.write_csv(args.out)

    print(f"Generated graph CSV files in: {args.out}")


if __name__ == "__main__":
    main()