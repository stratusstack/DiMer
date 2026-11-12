"""Canonicalization contract v1 — the Python reference implementation.

Normative rules: ADR-0001 §D3 (docs/adr/0001-comparison-contract.md). Every
row-equality path decides "equal" by comparing ``canonicalize()`` results;
``row_fingerprint()`` is only used to prune work between two sides that share
the same fingerprint recipe.
"""

import hashlib
import json
import math
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

CANONICALIZATION_VERSION = "1"

# Logical type families used to decide whether a column pair is comparable.
# Labels are DataTypeMapper common types; anything else is "unknown" and
# compatible with everything (non-SQL connectors report free-form types).
_TYPE_FAMILIES = {
    "string": "string", "text": "string", "uuid": "string",
    "int8": "numeric", "int16": "numeric", "int32": "numeric", "int64": "numeric",
    "uint8": "numeric", "uint16": "numeric", "uint32": "numeric", "uint64": "numeric",
    "float32": "numeric", "float64": "numeric", "decimal": "numeric",
    # booleans compare equal to numeric 1/0 (MySQL TINYINT(1))
    "boolean": "numeric",
    "date": "date",
    "datetime": "timestamp", "timestamp": "timestamp",
    "time": "time",
    "binary": "binary",
    "json": "structured", "array": "structured", "object": "structured",
}


def types_compatible(type_a: Optional[str], type_b: Optional[str]) -> bool:
    """True when two common-type labels can be compared under contract v1."""
    fam_a = _TYPE_FAMILIES.get((type_a or "").lower())
    fam_b = _TYPE_FAMILIES.get((type_b or "").lower())
    if fam_a is None or fam_b is None:
        return True
    return fam_a == fam_b


def _to_decimal(value: float) -> Decimal:
    """Shortest round-trip repr, so 0.1 (float) == 0.1 (numeric)."""
    return Decimal(repr(value))


def _normalize_decimal(value: Decimal) -> Decimal:
    if value.is_nan():
        raise InvalidOperation
    if value == 0:
        return Decimal(0)
    return value.normalize()


def canonicalize(value: Any) -> Any:
    """Return a comparable, hashable canonical form of ``value``.

    NULL (and NaN/NaT, which pandas-based transports use for NULL) becomes
    ``None``; every other value becomes a ``(tag, payload)`` tuple, so values
    of different logical families never compare equal.
    """
    if value is None:
        return None

    # pandas / numpy scalars → Python builtins
    if type(value).__name__ == "NaTType":
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime(warn=False)
    elif hasattr(value, "item") and getattr(value, "ndim", None) == 0:
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, bool):
        return ("num", Decimal(int(value)))
    if isinstance(value, int):
        return ("num", Decimal(value))
    if isinstance(value, float):
        if math.isinf(value):
            return ("num", Decimal(value))
        return ("num", _normalize_decimal(_to_decimal(value)))
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        if value.is_infinite():
            return ("num", value)
        return ("num", _normalize_decimal(value))
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return ("ts_tz", value.astimezone(timezone.utc).replace(tzinfo=None))
        return ("ts", value)
    if isinstance(value, date):
        return ("date", value)
    if isinstance(value, time):
        return ("time", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("bin", bytes(value))
    if isinstance(value, dict):
        return ("json", json.dumps(value, sort_keys=True, separators=(",", ":"), default=str))
    if isinstance(value, (list, tuple)) or hasattr(value, "tolist"):
        items = value.tolist() if hasattr(value, "tolist") else value
        return ("arr", tuple(canonicalize(v) for v in items))
    return ("other", str(value))


def canonical_row(row: Dict[str, Any], columns: List[str]) -> Tuple[Any, ...]:
    """Canonical values of ``columns`` (case-insensitive lookup) in order."""
    return tuple(canonicalize(_lookup(row, c)) for c in columns)


def _canonical_text(canon: Tuple[str, Any]) -> str:
    tag, payload = canon
    if tag == "num":
        return format(payload, "f") if payload.is_finite() else str(payload)
    if tag in ("ts", "ts_tz", "date", "time"):
        return payload.isoformat()
    if tag == "bin":
        return payload.hex()
    if tag == "arr":
        return "[" + ",".join(_frame(v) for v in payload) + "]"
    return str(payload)


def _frame(canon: Any) -> str:
    """``N`` for NULL, else ``<tag><len>:<text>`` — unambiguous field boundaries."""
    if canon is None:
        return "N"
    text = _canonical_text(canon)
    return f"{canon[0]}{len(text)}:{text}"


def row_fingerprint(row: Dict[str, Any], columns: List[str]) -> str:
    """MD5 over the framed canonical values of ``columns``.

    Only comparable with fingerprints produced by this function under the
    same ``CANONICALIZATION_VERSION``; never with SQL-side fingerprints.
    """
    raw = "".join(_frame(v) for v in canonical_row(row, columns))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _lookup(row: Dict[str, Any], col: str) -> Any:
    if col in row:
        return row[col]
    col_lower = col.lower()
    for k, v in row.items():
        if k.lower() == col_lower:
            return v
    return None
