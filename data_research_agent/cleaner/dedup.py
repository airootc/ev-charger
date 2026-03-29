"""Deduplication logic for raw records."""

from __future__ import annotations

from models import RawRecord


def deduplicate(
    records: list[RawRecord], keys: list[str]
) -> tuple[list[RawRecord], int]:
    """Remove duplicate records based on specified key fields.

    When duplicates exist, keeps the record with the latest fetched_at timestamp.

    Args:
        records: List of raw records to deduplicate.
        keys: Field names in raw_data to use as the dedup key.

    Returns:
        Tuple of (unique_records, duplicate_count).
    """
    if not keys:
        return records, 0

    seen: dict[tuple, RawRecord] = {}

    for record in records:
        key = _compute_key(record.raw_data, keys)
        if key in seen:
            existing = seen[key]
            # Keep the more recent one
            if record.fetched_at > existing.fetched_at:
                seen[key] = record
        else:
            seen[key] = record

    unique = list(seen.values())
    duplicate_count = len(records) - len(unique)
    return unique, duplicate_count


def _compute_key(data: dict, keys: list[str]) -> tuple:
    """Compute a hashable key from specified fields in a dict."""
    values = []
    for k in keys:
        val = data.get(k)
        if isinstance(val, str):
            val = val.strip().lower()
        values.append(val)
    return tuple(values)
