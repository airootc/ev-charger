"""Record validation using required fields and type checking."""

from __future__ import annotations

from models import FieldType, FlaggedRecord


def validate_records(
    records: list[dict],
    required_fields: list[str],
    field_types: dict[str, FieldType] | None = None,
) -> tuple[list[dict], list[FlaggedRecord]]:
    """Validate a list of record dicts.

    Checks:
    - Required fields are present and non-None
    - Field types match expectations (numeric fields are numeric, etc.)

    Args:
        records: List of normalized data dicts.
        required_fields: Fields that must be present and non-None.
        field_types: Optional mapping of field name -> expected type.

    Returns:
        Tuple of (valid_records, flagged_records).
    """
    valid = []
    flagged = []

    for record in records:
        source = record.get("source", record.get("_source", "unknown"))
        reasons = []

        # Check required fields
        for field in required_fields:
            value = record.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                reasons.append(f"missing required field: {field}")

        # Check field types
        if field_types:
            for field_name, field_type in field_types.items():
                value = record.get(field_name)
                if value is None:
                    continue  # Only validate non-None values

                type_error = _check_type(value, field_type, field_name)
                if type_error:
                    reasons.append(type_error)

        if reasons:
            flagged.append(FlaggedRecord(
                source=source,
                raw_data=record,
                flag_reason="; ".join(reasons),
            ))
        else:
            valid.append(record)

    return valid, flagged


def _check_type(value, field_type: FieldType, field_name: str) -> str | None:
    """Check if a value matches the expected field type. Returns error message or None."""
    if field_type in (FieldType.CURRENCY, FieldType.NUMBER):
        if not isinstance(value, (int, float)):
            return f"{field_name}: expected numeric, got {type(value).__name__}"
        if field_type == FieldType.CURRENCY and value < 0:
            return f"{field_name}: currency value is negative ({value})"

    elif field_type == FieldType.DATE:
        if not isinstance(value, str):
            return f"{field_name}: expected date string, got {type(value).__name__}"
        # Basic ISO date format check
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}", value):
            return f"{field_name}: date not in ISO format ({value})"

    elif field_type == FieldType.URL:
        if isinstance(value, str) and value and not value.startswith(("http://", "https://", "/")):
            return f"{field_name}: invalid URL format ({value[:50]})"

    return None
