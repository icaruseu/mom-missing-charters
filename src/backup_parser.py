"""eXist-db backup parser with dual source extraction."""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import final
from zipfile import ZipFile

from tqdm import tqdm

from .utils import is_charter_path, normalize_path


@final
class BackupParser:
    """Parser for eXist-db ZIP backups."""

    def __init__(self, backup_path: Path, charter_base_path: str):
        """Initialize backup parser.

        Args:
            backup_path: Path to backup ZIP file
            charter_base_path: Base path for charter collection (e.g., db/mom-data/metadata.charter.public)
        """
        self.backup_path = backup_path
        self.charter_base_path = charter_base_path

    def extract_charters(self) -> tuple[set[str], set[str], list[tuple[str, str]]]:
        """Extract charter paths from backup using dual sources.

        Returns:
            Tuple of:
            - Set of paths from __contents__.xml
            - Set of paths from actual ZIP entries
            - List of (normalized_path, raw_path) tuples for all unique charters
        """
        with ZipFile(self.backup_path, "r", metadata_encoding="utf-8") as zip_file:
            contents_xml_paths = self._extract_from_contents_xml(zip_file)
            zip_entry_paths = self._extract_from_zip_entries(zip_file)

        all_raw_paths = contents_xml_paths | zip_entry_paths
        path_mapping = []
        seen_normalized = set()

        with tqdm(
            total=len(all_raw_paths),
            desc="  Normalizing paths",
            unit="path",
            leave=False,
        ) as pbar:
            for raw_path in all_raw_paths:
                pbar.update(1)
                normalized = normalize_path(raw_path)
                if normalized not in seen_normalized:
                    path_mapping.append((normalized, raw_path))
                    seen_normalized.add(normalized)

        return contents_xml_paths, zip_entry_paths, path_mapping

    def _extract_from_contents_xml(self, zip_file: ZipFile) -> set[str]:
        """Extract charter paths from __contents__.xml files.

        Args:
            zip_file: Open ZipFile object

        Returns:
            Set of charter file paths found in __contents__.xml files
        """
        charter_paths = set()
        contents_files = [
            name for name in zip_file.namelist() if name.endswith("__contents__.xml")
        ]

        with tqdm(
            total=len(contents_files),
            desc="  Parsing __contents__.xml",
            unit="file",
            leave=False,
        ) as pbar:
            for contents_file in contents_files:
                pbar.update(1)

                try:
                    with zip_file.open(contents_file) as f:
                        tree = ET.parse(f)
                        root = tree.getroot()

                    collection_path = root.get("name")

                    if not collection_path:
                        collection_path = str(Path(contents_file).parent)
                    else:
                        if collection_path.startswith("/"):
                            collection_path = collection_path[1:]

                    collection_path = collection_path.rstrip("/")

                    namespace = (
                        root.tag.split("}")[0].strip("{") if "}" in root.tag else None
                    )

                    if namespace:
                        resources = root.findall(f".//{{{namespace}}}resource")
                    else:
                        resources = root.findall(".//resource")

                    for resource in resources:
                        filename = resource.get("name")
                        if filename and filename.endswith(".xml"):
                            filename = filename.lstrip("/")
                            full_path = f"{collection_path}/{filename}"

                            if is_charter_path(full_path, self.charter_base_path):
                                charter_paths.add(full_path)

                except ET.ParseError:
                    continue
                except Exception:
                    continue

        return charter_paths

    def _extract_from_zip_entries(self, zip_file: ZipFile) -> set[str]:
        """Extract charter paths from actual ZIP file entries.

        Args:
            zip_file: Open ZipFile object

        Returns:
            Set of charter file paths found in ZIP entries
        """
        charter_paths = set()
        all_entries = zip_file.namelist()

        with tqdm(
            total=len(all_entries),
            desc="  Scanning ZIP entries",
            unit="entry",
            leave=False,
        ) as pbar:
            for entry in all_entries:
                pbar.update(1)

                if entry.endswith("__contents__.xml"):
                    continue

                if is_charter_path(entry, self.charter_base_path):
                    charter_paths.add(entry)

        return charter_paths

    def get_discrepancies(
        self,
        contents_xml_paths: set[str],
        zip_entry_paths: set[str],
    ) -> list[dict]:
        """Find discrepancies between __contents__.xml and actual ZIP entries.

        Args:
            contents_xml_paths: Paths from __contents__.xml (raw paths)
            zip_entry_paths: Paths from ZIP entries (raw paths)

        Returns:
            List of discrepancy records
        """
        contents_normalized: dict[str, set[str]] = {}
        for raw_path in contents_xml_paths:
            norm = normalize_path(raw_path)
            if norm not in contents_normalized:
                contents_normalized[norm] = set()
            contents_normalized[norm].add(raw_path)

        zip_normalized: dict[str, set[str]] = {}
        for raw_path in zip_entry_paths:
            norm = normalize_path(raw_path)
            if norm not in zip_normalized:
                zip_normalized[norm] = set()
            zip_normalized[norm].add(raw_path)

        discrepancies = []

        only_in_contents = set(contents_normalized.keys()) - set(zip_normalized.keys())
        for norm_path in only_in_contents:
            raw_path = next(iter(contents_normalized[norm_path]))
            discrepancies.append(
                {
                    "file_path": norm_path,
                    "in_contents_xml": True,
                    "in_zip_entries": False,
                }
            )

        only_in_zip = set(zip_normalized.keys()) - set(contents_normalized.keys())
        for norm_path in only_in_zip:
            discrepancies.append(
                {
                    "file_path": norm_path,
                    "in_contents_xml": False,
                    "in_zip_entries": True,
                }
            )

        return discrepancies

