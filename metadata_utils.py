from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://ghoapi.azureedge.net/api/Indicator"
TIMEOUT_SECONDS = 45


def _session() -> requests.Session:
    """Build a session with retry/backoff for transient API/network failures."""
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {"User-Agent": "WHO-GHO-SNI-metadata-collection/1.0"}
    )
    return session


def fetch_indicator_catalog(
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Download all pages from the WHO GHO Indicator catalog.

    Supports either @odata.nextLink or @data.nextLink pagination fields.
    Raises an explicit error if the catalog is empty or lacks expected fields.
    """
    own_session = session is None
    session = session or _session()
    url = API_URL
    records: list[dict] = []
    seen_urls: set[str] = set()

    try:
        while url:
            if url in seen_urls:
                raise RuntimeError(f"Pagination loop detected at {url}")
            seen_urls.add(url)

            response = session.get(url, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            page = payload.get("value", [])
            if not isinstance(page, list):
                raise ValueError(
                    "WHO API response field 'value' was not a list"
                )

            records.extend(
                row for row in page if isinstance(row, dict)
            )
            url = (
                payload.get("@odata.nextLink")
                or payload.get("@data.nextLink")
            )
            print(f"Retrieved {len(records):,} catalog records so far...")
    finally:
        if own_session:
            session.close()

    if not records:
        raise RuntimeError(
            "WHO GHO Indicator catalog returned no records."
        )

    catalog = pd.DataFrame(records)

    if (
        "IndicatorCode" not in catalog.columns
        or "IndicatorName" not in catalog.columns
    ):
        raise ValueError(
            "WHO catalog is missing expected IndicatorCode/IndicatorName "
            f"fields. Fields received: {catalog.columns.tolist()}"
        )

    catalog = catalog.drop_duplicates(
        subset=["IndicatorCode"], keep="first"
    )
    return catalog.reset_index(drop=True)


def filter_topic_metadata(
    catalog: pd.DataFrame,
    topic: str,
    keywords: Iterable[str],
) -> pd.DataFrame:
    """Filter catalog rows by literal, case-insensitive IndicatorName terms.

    Records every configured search term responsible for a match.
    """
    terms = [
        str(term).strip()
        for term in keywords
        if str(term).strip()
    ]

    if not terms:
        raise ValueError(
            f"No search terms configured for topic: {topic}"
        )

    if "IndicatorName" not in catalog.columns:
        raise ValueError("Catalog has no IndicatorName column")

    names = catalog["IndicatorName"].fillna("").astype(str)

    term_masks = {
        term: names.str.contains(
            re.escape(term), case=False, regex=True
        )
        for term in terms
    }

    any_match = pd.Series(False, index=catalog.index)
    for mask in term_masks.values():
        any_match |= mask

    result = catalog.loc[any_match].copy()

    if result.empty:
        print(
            f"WARNING: No indicators matched the configured terms "
            f"for {topic}."
        )
        result["MatchedTerms"] = pd.Series(dtype="object")
        result["Topic"] = pd.Series(dtype="object")
        result["RetrievedAtUTC"] = pd.Series(dtype="object")
        return result

    result["MatchedTerms"] = [
        "; ".join(
            term
            for term, mask in term_masks.items()
            if bool(mask.loc[idx])
        )
        for idx in result.index
    ]

    result["Topic"] = topic
    result["RetrievedAtUTC"] = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")

    sort_cols = [
        col
        for col in ["IndicatorCode", "IndicatorName"]
        if col in result.columns
    ]

    return (
        result.drop_duplicates(
            subset=["IndicatorCode"], keep="first"
        )
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )


def run_metadata_job(
    topic: str,
    keywords: Iterable[str],
    output_path: str | Path,
) -> pd.DataFrame:
    """Fetch catalog, filter it, and save a topic-specific metadata CSV."""
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching WHO GHO indicator metadata for: {topic}")

    catalog = fetch_indicator_catalog()
    metadata = filter_topic_metadata(
        catalog, topic, keywords
    )

    metadata.to_csv(
        output_path, index=False, encoding="utf-8-sig"
    )

    print(f"Catalog indicators: {len(catalog):,}")
    print(
        f"Matched unique indicators for {topic}: "
        f"{len(metadata):,}"
    )
    print(f"Saved metadata CSV: {output_path}")
    print("Metadata fields:", ", ".join(metadata.columns))

    return metadata