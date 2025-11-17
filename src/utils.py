"""Utility functions for path normalization and date parsing."""

import html
import re
import unicodedata
from datetime import datetime
from urllib.parse import unquote


def normalize_path(path: str) -> str:
    """Normalize a charter file path for consistent comparison.

    Handles encoding variations between __contents__.xml and ZIP entries:
    - eXist-db custom encoding (&XX; hex codes)
    - HTML entity decoding
    - URL encoding
    - Plus signs as spaces
    - Unicode normalization to NFC form
    - Path separator and whitespace normalization

    Args:
        path: Raw path from backup

    Returns:
        Normalized path
    """
    path = path.strip()

    def decode_exist_encoding(text: str) -> str:
        """Decode eXist-db's custom &XX; hex encoding."""

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
