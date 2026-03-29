"""Data normalization functions for cleaning raw records.

Provides field-level normalizers (text, date, number, URL, etc.) plus
domain-specific normalizers for EV charging data:

- **Connector type standardization** — maps common variations of connector
  names to a controlled vocabulary (Type 2, CCS2, CHAdeMO, NACS, Type 1).
- **Country code normalization** — maps full country names / common
  variants to ISO 3166-1 alpha-2 codes.
- **num_ports casting** — safely converts float-like strings ("2.0") to int.
"""

from __future__ import annotations

import math
import re
import unicodedata
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from dateutil import parser as date_parser

from models import FieldType

# Tracking params to strip from URLs
TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid", "ref", "ref_src",
}

# ---------------------------------------------------------------------------
# Connector type standardization
# ---------------------------------------------------------------------------
# Maps common variations (case-insensitive) to a controlled vocabulary.
# The canonical names include AC/DC designation for clarity.
CONNECTOR_TYPE_MAP: dict[str, str] = {
    # Type 2 (AC) — IEC 62196-2
    "type 2":       "Type 2 (AC)",
    "typ 2":        "Type 2 (AC)",
    "iec 62196":    "Type 2 (AC)",
    "iec62196":     "Type 2 (AC)",
    "mennekes":     "Type 2 (AC)",
    # CCS2 (DC) — Combined Charging System, Type 2 combo
    "ccs":          "CCS2 (DC)",
    "ccs2":         "CCS2 (DC)",
    "ccs combo 2":  "CCS2 (DC)",
    "ccs (type 2)": "CCS2 (DC)",
    "combined charging system": "CCS2 (DC)",
    # CHAdeMO (DC)
    "chademo":      "CHAdeMO (DC)",
    # NACS / Tesla
    "nacs":         "NACS/Tesla",
    "nacs (tesla)": "NACS/Tesla",
    "tesla":        "NACS/Tesla",
    "tesla supercharger": "NACS/Tesla",
    # Type 1 / J1772 (AC)
    "type 1":       "Type 1/J1772 (AC)",
    "j1772":        "Type 1/J1772 (AC)",
    "sae j1772":    "Type 1/J1772 (AC)",
    "type 1 (j1772)": "Type 1/J1772 (AC)",
    # CCS1 (DC) — Combined Charging System, Type 1 combo
    "ccs1":         "CCS1 (DC)",
    "ccs (type 1)": "CCS1 (DC)",
    "ccs combo 1":  "CCS1 (DC)",
}

# ---------------------------------------------------------------------------
# Country name -> ISO 3166-1 alpha-2 mapping
# ---------------------------------------------------------------------------
# Common full names and variants encountered in source data.
COUNTRY_NAME_TO_CODE: dict[str, str] = {
    "united states": "US",
    "united states of america": "US",
    "usa": "US",
    "u.s.a.": "US",
    "u.s.": "US",
    "canada": "CA",
    "united kingdom": "GB",
    "great britain": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "germany": "DE",
    "deutschland": "DE",
    "france": "FR",
    "italy": "IT",
    "italia": "IT",
    "spain": "ES",
    "españa": "ES",
    "portugal": "PT",
    "netherlands": "NL",
    "holland": "NL",
    "belgium": "BE",
    "austria": "AT",
    "österreich": "AT",
    "switzerland": "CH",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "finland": "FI",
    "ireland": "IE",
    "poland": "PL",
    "czech republic": "CZ",
    "czechia": "CZ",
    "hungary": "HU",
    "romania": "RO",
    "greece": "GR",
    "turkey": "TR",
    "türkiye": "TR",
    "russia": "RU",
    "china": "CN",
    "japan": "JP",
    "south korea": "KR",
    "korea": "KR",
    "republic of korea": "KR",
    "india": "IN",
    "australia": "AU",
    "new zealand": "NZ",
    "brazil": "BR",
    "mexico": "MX",
    "argentina": "AR",
    "chile": "CL",
    "colombia": "CO",
    "south africa": "ZA",
    "egypt": "EG",
    "kenya": "KE",
    "nigeria": "NG",
    "israel": "IL",
    "saudi arabia": "SA",
    "united arab emirates": "AE",
    "uae": "AE",
    "singapore": "SG",
    "malaysia": "MY",
    "thailand": "TH",
    "indonesia": "ID",
    "philippines": "PH",
    "vietnam": "VN",
    "taiwan": "TW",
    "hong kong": "HK",
    "iceland": "IS",
    "luxembourg": "LU",
    "croatia": "HR",
    "slovenia": "SI",
    "slovakia": "SK",
    "bulgaria": "BG",
    "serbia": "RS",
    "estonia": "EE",
    "latvia": "LV",
    "lithuania": "LT",
    "cyprus": "CY",
    "malta": "MT",
}


def normalize_date(value) -> str | None:
    """Parse various date formats into ISO 8601 (YYYY-MM-DD).

    Handles: "Mar 5, 2025", "2025-03-05", "03/05/2025", "5 March 2025",
    timestamps, etc.
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    try:
        parsed = date_parser.parse(value, fuzzy=True)
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def normalize_currency(value) -> float | None:
    """Convert currency strings to float.

    Handles: "$1,500", "1500", "1.5k", "$1.5K", "2.3M", "$2,300,000", "150K"
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    # Remove currency symbols and whitespace
    cleaned = re.sub(r"[£€¥₹$\s]", "", value)

    # Handle K/M/B suffixes
    multiplier = 1.0
    if cleaned and cleaned[-1].upper() in ("K", "M", "B"):
        suffix = cleaned[-1].upper()
        cleaned = cleaned[:-1]
        if suffix == "K":
            multiplier = 1_000
        elif suffix == "M":
            multiplier = 1_000_000
        elif suffix == "B":
            multiplier = 1_000_000_000

    # Remove commas
    cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned) * multiplier
    except (ValueError, TypeError):
        return None


def normalize_text(value) -> str | None:
    """Normalize text: strip, normalize unicode, collapse whitespace."""
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    # Normalize unicode (NFKC)
    value = unicodedata.normalize("NFKC", value)

    # Collapse multiple whitespace
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalize_url(value) -> str | None:
    """Validate URL format and strip common tracking parameters."""
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    # Basic URL validation
    if not value.startswith(("http://", "https://", "//")):
        return value  # Return as-is for relative URLs

    try:
        parsed = urlparse(value)
        if not parsed.netloc:
            return value

        # Remove tracking params
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            cleaned_params = {
                k: v for k, v in params.items()
                if k.lower() not in TRACKING_PARAMS
            }
            clean_query = urlencode(cleaned_params, doseq=True)
            parsed = parsed._replace(query=clean_query)

        # Remove fragment
        parsed = parsed._replace(fragment="")

        return urlunparse(parsed)
    except Exception:
        return value


def normalize_location(value) -> str | None:
    """Basic location normalization: strip whitespace, title case."""
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)

    # Title case, but preserve common abbreviations
    parts = value.split(",")
    normalized = []
    for part in parts:
        part = part.strip()
        # Keep 2-3 letter tokens uppercase (state/country codes)
        if len(part) <= 3:
            normalized.append(part.upper())
        else:
            normalized.append(part.title())

    return ", ".join(normalized)


def normalize_number(value) -> float | None:
    """Parse a numeric value."""
    if value is None:
        return None

    value = str(value).strip().replace(",", "")
    if not value:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def normalize_connector_type(value: str | None) -> str | None:
    """Standardize EV connector type names to a controlled vocabulary.

    Handles comma-separated lists of connector types (e.g. from NREL's
    ``ev_connector_types`` field).  Each individual type is looked up
    case-insensitively in ``CONNECTOR_TYPE_MAP``; unrecognised values are
    passed through unchanged.

    Examples:
        >>> normalize_connector_type("CCS2")
        'CCS2 (DC)'
        >>> normalize_connector_type("Type 2, CHAdeMO")
        'Type 2 (AC), CHAdeMO (DC)'
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    parts = [p.strip() for p in value.split(",")]
    normalised: list[str] = []
    seen: set[str] = set()

    for part in parts:
        if not part:
            continue
        canonical = CONNECTOR_TYPE_MAP.get(part.lower(), part)
        if canonical not in seen:
            normalised.append(canonical)
            seen.add(canonical)

    return ", ".join(normalised) if normalised else None


def normalize_country_code(value: str | None) -> str | None:
    """Normalize a country value to an ISO 3166-1 alpha-2 code.

    If the value is already a 2-letter code it is uppercased and returned.
    Otherwise the value is looked up (case-insensitively) in
    ``COUNTRY_NAME_TO_CODE``.  Unrecognised values are returned as-is after
    stripping / normalizing whitespace.

    Examples:
        >>> normalize_country_code("United States")
        'US'
        >>> normalize_country_code("de")
        'DE'
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    # Already a 2-letter code
    if len(value) == 2 and value.isalpha():
        return value.upper()

    lookup = value.lower()
    code = COUNTRY_NAME_TO_CODE.get(lookup)
    if code:
        return code

    # Return original text when no mapping exists (don't destroy data)
    return value


def normalize_num_ports(value) -> int | None:
    """Cast a port count to int, handling float strings like '2.0'.

    Returns ``None`` for non-numeric or non-positive values.

    Examples:
        >>> normalize_num_ports("2.0")
        2
        >>> normalize_num_ports(3)
        3
    """
    if value is None:
        return None

    try:
        f = float(str(value).strip())
        if math.isnan(f) or math.isinf(f) or f < 0:
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


# Map field types to normalizer functions
NORMALIZERS = {
    FieldType.TEXT: normalize_text,
    FieldType.DATE: normalize_date,
    FieldType.CURRENCY: normalize_currency,
    FieldType.URL: normalize_url,
    FieldType.LOCATION: normalize_location,
    FieldType.NUMBER: normalize_number,
}

# Fields that receive domain-specific normalization beyond their FieldType.
# Applied *after* the generic type normalizer in normalize_record().
_DOMAIN_NORMALIZERS: dict[str, callable] = {
    "connector_types": normalize_connector_type,
    "country": normalize_country_code,
    "country_code": normalize_country_code,
    "num_ports": normalize_num_ports,
    "num_level1_ports": normalize_num_ports,
    "num_level2_ports": normalize_num_ports,
    "num_dc_fast_ports": normalize_num_ports,
}


def normalize_record(
    data: dict, field_types: dict[str, FieldType]
) -> tuple[dict, dict[str, int]]:
    """Normalize all fields in a record based on their declared types.

    Two normalization passes are applied:

    1. **Generic type normalization** — each field is processed by the
       normalizer registered for its ``FieldType`` (text, number, date, ...).
    2. **Domain-specific normalization** — fields listed in
       ``_DOMAIN_NORMALIZERS`` receive additional EV-specific processing
       (connector type standardization, country code mapping, port count
       casting).

    Args:
        data: Raw record data dict.
        field_types: Mapping of field name -> FieldType.

    Returns:
        Tuple of (normalized_data, fields_fixed_counts).
        fields_fixed_counts tracks how many fields were changed by normalization.
    """
    result = dict(data)  # shallow copy
    fields_fixed: dict[str, int] = {}

    # Pass 1: generic type normalization
    for field_name, field_type in field_types.items():
        if field_name not in result:
            continue

        original = result[field_name]
        normalizer = NORMALIZERS.get(field_type, normalize_text)
        normalized = normalizer(original)

        if normalized != original and original is not None:
            fields_fixed[field_name] = fields_fixed.get(field_name, 0) + 1

        result[field_name] = normalized

    # Pass 2: domain-specific normalization
    for field_name, domain_normalizer in _DOMAIN_NORMALIZERS.items():
        if field_name not in result:
            continue

        original = result[field_name]
        normalized = domain_normalizer(original)

        if normalized != original and original is not None:
            fields_fixed[field_name] = fields_fixed.get(field_name, 0) + 1

        result[field_name] = normalized

    return result, fields_fixed
