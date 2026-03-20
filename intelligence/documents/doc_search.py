"""
Lightweight keyword search module over the document_index SQLite table.

Usage:
    python -m intelligence.documents.doc_search "rate increase policy"
"""

import re
import sqlite3
import sys
from typing import Optional

STOP_WORDS = {"the", "a", "an", "is", "are", "in", "on", "at", "for", "to", "of", "and", "or"}

ALERT_KEYWORD_MAP = {
    "staffing": "employee hiring schedule coverage overtime",
    "pricing": "rate card pricing service cost increase",
    "onboarding": "onboarding checklist new client welcome",
    "quality": "quality inspection cleaning standards SOP",
    "safety": "safety training equipment PPE",
    "commercial": "commercial contract terms agreement scope",
    "referral": "referral program reward bonus",
    "supply": "supply inventory cleaning products order",
}


def _tokenize(text: str) -> list[str]:
    """Lowercase and strip punctuation, then split into words."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return [w for w in text.split() if w]


def _keywords_from_query(query: str) -> list[str]:
    """Tokenize query and remove stop words."""
    return [w for w in _tokenize(query) if w not in STOP_WORDS]


def extract_relevant_excerpt(content: str, keywords: list[str], max_length: int = 500) -> str:
    """Find the paragraph with the highest keyword density.

    Splits content on double newlines, scores each paragraph by keyword
    occurrence count, and returns the top-scoring paragraph truncated to
    max_length characters.
    """
    if not content:
        return ""

    paragraphs = [p.strip() for p in re.split(r"\n\n+", content) if p.strip()]
    if not paragraphs:
        # Fall back to a sliding window when there are no paragraph breaks
        paragraphs = [content[i : i + max_length] for i in range(0, len(content), max_length)]

    def score_paragraph(para: str) -> int:
        lower = para.lower()
        return sum(lower.count(kw) for kw in keywords)

    best = max(paragraphs, key=score_paragraph)

    if len(best) <= max_length:
        return best

    # Truncate at a word boundary
    truncated = best[:max_length].rsplit(" ", 1)[0]
    return truncated + "..."


def search_documents(db_path: str, query: str, max_results: int = 3) -> list[dict]:
    """Search documents by keyword matching.

    Tokenizes the query, removes stop words, then issues a LIKE query for
    each remaining keyword against content_text.  Results are scored by the
    number of distinct keywords that matched and returned sorted descending.
    """
    keywords = _keywords_from_query(query)
    if not keywords:
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Build a query that returns rows matching ANY keyword, then score
        # in Python so we can count how many distinct keywords matched.
        # Actual column names in document_index: chunk_text, source_title, indexed_at
        like_clauses = " OR ".join("chunk_text LIKE ?" for _ in keywords)
        params = [f"%{kw}%" for kw in keywords]
        sql = f"""
            SELECT doc_id, source_title, chunk_text
            FROM document_index
            WHERE {like_clauses}
        """
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    scored = []
    for row in rows:
        content_lower = (row["chunk_text"] or "").lower()
        match_score = sum(1 for kw in keywords if kw in content_lower)
        excerpt = extract_relevant_excerpt(row["chunk_text"] or "", keywords)
        scored.append(
            {
                "doc_id": row["doc_id"],
                "title": row["source_title"],
                "source": None,
                "match_score": match_score,
                "relevant_excerpt": excerpt,
            }
        )

    scored.sort(key=lambda d: d["match_score"], reverse=True)
    return scored[:max_results]


def get_document_by_id(db_path: str, doc_id: str) -> Optional[dict]:
    """Retrieve a single document's full content by ID."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM document_index WHERE doc_id = ?", (doc_id,)
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return dict(row)


def search_for_alert_context(db_path: str, alert_text: str) -> list[dict]:
    """Given an alert string, find the best matching keyword set and search.

    Checks each key in ALERT_KEYWORD_MAP against the alert text
    (case-insensitive). Uses the first matching category's query. Falls back
    to the first 5 non-stop-words from alert_text if nothing matches.
    """
    alert_lower = alert_text.lower()

    query = None
    for key, mapped_query in ALERT_KEYWORD_MAP.items():
        if key in alert_lower:
            query = mapped_query
            break

    if query is None:
        fallback_words = _keywords_from_query(alert_text)[:5]
        query = " ".join(fallback_words)

    if not query:
        return []

    return search_documents(db_path, query)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m intelligence.documents.doc_search <query>")
        sys.exit(1)

    import os

    query = " ".join(sys.argv[1:])

    # Resolve db_path relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(project_root, "sparkle_shine.db")

    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        sys.exit(1)

    results = search_documents(db_path, query)

    if not results:
        print(f"No documents found for query: '{query}'")
        return

    print(f"Search results for: '{query}'\n{'=' * 60}")
    for i, doc in enumerate(results, 1):
        print(f"\n[{i}] {doc['title']}  (source: {doc['source']}, score: {doc['match_score']})")
        print(f"    doc_id: {doc['doc_id']}")
        print(f"    Excerpt: {doc['relevant_excerpt']}")


if __name__ == "__main__":
    _main()
