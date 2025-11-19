"""Utility functions for path normalization and date parsing."""

import html
import re
import unicodedata
from datetime import datetime
from urllib.parse import unquote


def normalize_path(path: str) -> str:
    """Normalize a charter file path for consistent comparison.

    Handles eXist-db custom encoding, HTML entities, URL encoding,
    Unicode normalization (NFC), and path separator normalization.

    Args:
        path: Raw path from backup

    Returns:
        Normalized path
    """
    path = path.strip()

    def decode_exist_encoding(text: str) -> str:
        def replace_hex(match):
            hex_code = match.group(1)
            try:
                return chr(int(hex_code, 16))
            except (ValueError, OverflowError):
                return match.group(0)

        return re.sub(r"&([0-9A-Fa-f]{2});", replace_hex, text)

    decoded = decode_exist_encoding(path)

    try:
        decoded = html.unescape(decoded)
    except Exception:
        pass

    try:
        prev_decoded = decoded
        while True:
            decoded = unquote(decoded, errors="strict")
            if decoded == prev_decoded:
                break
            prev_decoded = decoded
    except Exception:
        pass

    decoded = decoded.replace("+", " ")
    normalized = unicodedata.normalize("NFC", decoded)
    normalized = normalized.replace("\\", "/")

    while "//" in normalized:
        normalized = normalized.replace("//", "/")

    while "  " in normalized:
        normalized = normalized.replace("  ", " ")

    if len(normalized) > 1 and normalized.endswith("/"):
        normalized = normalized.rstrip("/")

    return normalized


def parse_backup_filename(filename: str) -> datetime | None:
    """Parse backup filename to extract date.

    Expected format: fullYYYYMMDD-HHMM.zip

    Args:
        filename: Backup filename

    Returns:
        Datetime object or None if parsing fails
    """
    pattern = r"full(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})\.zip"
    match = re.match(pattern, filename)

    if not match:
        return None

    year, month, day, hour, minute = match.groups()

    try:
        return datetime(int(year), int(month), int(day), int(hour), int(minute))
    except ValueError:
        return None


def is_charter_path(path: str, base_path: str) -> bool:
    """Check if a path is under the charter collection.

    Args:
        path: File path to check
        base_path: Base charter collection path

    Returns:
        True if path is under base_path and is an XML file
    """
    norm_path = normalize_path(path)
    norm_base = normalize_path(base_path)
    return norm_path.startswith(norm_base) and norm_path.endswith(".xml")


def format_datetime(dt: datetime) -> str:
    """Format datetime to ISO string.

    Args:
        dt: Datetime object

    Returns:
        ISO format string
    """
    return dt.isoformat()


def extract_parent_path(file_path: str, base_path: str) -> str:
    """Extract parent collection path from charter file path.

    Args:
        file_path: Full charter file path
        base_path: Base charter collection path

    Returns:
        Parent collection path (empty if at root level)
    """
    norm_path = normalize_path(file_path)
    norm_base = normalize_path(base_path)

    if norm_base.endswith("/"):
        norm_base = norm_base.rstrip("/")

    if not norm_path.startswith(norm_base):
        return ""

    relative_path = norm_path[len(norm_base) :].lstrip("/")

    if "/" in relative_path:
        return relative_path.rsplit("/", 1)[0]
    else:
        return ""


def encode_exist_path(path: str) -> str:
    """Encode special characters using eXist-db's &XX; hex encoding.

    Args:
        path: Path with special characters

    Returns:
        Path with encoded special characters
    """
    special_chars = {'|': '&7C;'}

    result = path
    for char, encoded in special_chars.items():
        result = result.replace(char, encoded)
    return result


def encode_path_latin1_corruption(path: str) -> str:
    """Simulate UTF-8 bytes misinterpreted as Latin-1.

    Args:
        path: Path with Unicode characters

    Returns:
        Path with simulated encoding corruption
    """
    try:
        return path.encode('utf-8').decode('latin-1', errors='replace')
    except Exception:
        return path


def encode_path_cp437_corruption(path: str) -> str:
    """Simulate UTF-8 → CP437 → UTF-8 double encoding corruption.

    Args:
        path: Path with Unicode characters

    Returns:
        Path with simulated double encoding
    """
    try:
        utf8_bytes = path.encode('utf-8')
        return utf8_bytes.decode('cp437', errors='replace')
    except Exception:
        return path


def generate_path_variants(normalized_path: str, raw_path: str | None) -> list[str]:
    """Generate all possible encoding variants of a path.

    Args:
        normalized_path: Normalized path
        raw_path: Original raw path from backup

    Returns:
        List of path variants, ordered by likelihood
    """
    from urllib.parse import quote

    variants = []

    if raw_path:
        variants.append(raw_path)

    if normalized_path not in variants:
        variants.append(normalized_path)

    exist_encoded = encode_exist_path(normalized_path)
    if exist_encoded not in variants:
        variants.append(exist_encoded)

    url_encoded = quote(normalized_path, safe='/')
    if url_encoded not in variants:
        variants.append(url_encoded)

    cp437_corrupted = encode_path_cp437_corruption(normalized_path)
    if cp437_corrupted not in variants and cp437_corrupted != normalized_path:
        variants.append(cp437_corrupted)

    exist_then_cp437 = encode_path_cp437_corruption(exist_encoded)
    if exist_then_cp437 not in variants and exist_then_cp437 != exist_encoded:
        variants.append(exist_then_cp437)

    latin1_corrupted = encode_path_latin1_corruption(normalized_path)
    if latin1_corrupted not in variants and latin1_corrupted != normalized_path:
        variants.append(latin1_corrupted)

    exist_then_latin1 = encode_path_latin1_corruption(exist_encoded)
    if exist_then_latin1 not in variants and exist_then_latin1 != exist_encoded:
        variants.append(exist_then_latin1)

    return variants


def should_process_backup(backup_index: int, frequency: int) -> bool:
    """Determine if a backup should be processed based on frequency.

    Args:
        backup_index: Index in sorted backup list (0-based)
        frequency: Process every Nth backup

    Returns:
        True if should be processed
    """
    if frequency <= 1:
        return True
    return backup_index % frequency == 0
