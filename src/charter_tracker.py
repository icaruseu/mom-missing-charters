"""Charter lifecycle tracking logic."""

import logging
import time
from pathlib import Path
from typing import final

from .backup_parser import BackupParser
from .database import Database
from .utils import extract_parent_path, format_datetime, parse_backup_filename

# Set up logger
logger = logging.getLogger(__name__)


@final
class CharterTracker:
    """Tracks charter lifecycle across backups."""

    def __init__(self, db: Database, charter_base_path: str):
        """Initialize charter tracker.

        Args:
            db: Database instance
            charter_base_path: Base path for charter collection
        """
        self.db = db
        self.charter_base_path = charter_base_path
        self.current_state: dict[str, int] = {}  # normalized_path -> charter_id

    def load_current_state(self):
        """Load current charter state from database."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT id, file_path, current_status
            FROM charters
            WHERE current_status = 'present'
        """)

        self.current_state = {}
        for row in cursor.fetchall():
            charter_id, file_path, status = row
            if status == "present":
                self.current_state[file_path] = charter_id

    def process_backup(self, backup_path: Path, backup_filename: str) -> dict:
        """Process a single backup file.

        Args:
            backup_path: Path to backup ZIP file
            backup_filename: Backup filename

        Returns:
            Processing statistics
        """
        start_time = time.time()

        # Parse backup date from filename
        backup_date = parse_backup_filename(backup_filename)
        if not backup_date:
            raise ValueError(f"Could not parse backup date from: {backup_filename}")

        backup_date_str = format_datetime(backup_date)

        # Add backup to database
        backup_id = self.db.add_backup(backup_filename, backup_date_str)

        cursor = self.db.conn.cursor()
        cursor.execute("BEGIN TRANSACTION")

        try:
            parse_start = time.time()
            parser = BackupParser(backup_path, self.charter_base_path)
            contents_xml_paths, zip_entry_paths, path_mapping = (
                parser.extract_charters()
            )
            logger.info(
                f"[ZIP] Extracted {len(path_mapping):,} charters in {time.time() - parse_start:.1f}s"
            )

            discrepancies = parser.get_discrepancies(
                contents_xml_paths, zip_entry_paths
            )
            if discrepancies:
                disc_start = time.time()
                discrepancy_tuples = [
                    (
                        backup_id,
                        disc["file_path"],
                        disc["in_contents_xml"],
                        disc["in_zip_entries"],
                    )
                    for disc in discrepancies
                ]
                self.db.add_discrepancies_batch(discrepancy_tuples)
                logger.info(
                    f"[DB] Inserted {len(discrepancies):,} discrepancies in {time.time() - disc_start:.2f}s"
                )

            current_backup_paths = {path for path, _ in path_mapping}

            categorize_start = time.time()
            new_charters = []
            existing_charter_ids = []
            paths_to_check_for_reappearance = []

            for normalized_path, raw_path in path_mapping:
                if normalized_path not in self.current_state:
                    paths_to_check_for_reappearance.append((normalized_path, raw_path))
                else:
                    existing_charter_ids.append(self.current_state[normalized_path])

            logger.info(
                f"[MEM] Categorized in {time.time() - categorize_start:.2f}s: "
                f"{len(existing_charter_ids):,} existing, {len(paths_to_check_for_reappearance):,} to check"
            )

            if paths_to_check_for_reappearance:
                num_to_check = len(paths_to_check_for_reappearance)
                query_start = time.time()
                existing_charters_map = self.db.get_charters_by_paths_batch(
                    [path for path, _ in paths_to_check_for_reappearance]
                )
                logger.info(
                    f"[DB] Queried {num_to_check:,} charters in {time.time() - query_start:.2f}s (found {len(existing_charters_map):,})"
                )
            else:
                existing_charters_map = {}

            appeared_events = []
            reappeared_count = 0

            for normalized_path, raw_path in paths_to_check_for_reappearance:
                charter = existing_charters_map.get(normalized_path)
                if charter:
                    if charter["current_status"] == "missing":
                        charter_id = charter["id"]
                        existing_charter_ids.append(charter_id)
                        self.current_state[normalized_path] = charter_id
                        appeared_events.append(
                            (charter_id, backup_id, "appeared", backup_date_str)
                        )
                        reappeared_count += 1
                    else:
                        self.current_state[normalized_path] = charter["id"]
                        existing_charter_ids.append(charter["id"])
                else:
                    parent_path = extract_parent_path(
                        normalized_path, self.charter_base_path
                    )
                    new_charters.append(
                        (normalized_path, raw_path, parent_path, backup_id)
                    )

            appeared_count = len(new_charters)
            if new_charters:
                num_new = len(new_charters)
                insert_start = time.time()
                charter_ids = self.db.add_charters_batch(new_charters)
                logger.info(
                    f"[DB] Inserted {num_new:,} new charters in {time.time() - insert_start:.2f}s"
                )

                for (normalized_path, _, _, _), charter_id in zip(
                    new_charters, charter_ids
                ):
                    self.current_state[normalized_path] = charter_id
                    appeared_events.append(
                        (charter_id, backup_id, "appeared", backup_date_str)
                    )

            if existing_charter_ids:
                update_start = time.time()
                self.db.update_charters_last_seen_batch(existing_charter_ids, backup_id)
                logger.info(
                    f"[DB] Updated {len(existing_charter_ids):,} existing charters in {time.time() - update_start:.2f}s"
                )

            if appeared_events:
                event_start = time.time()
                self.db.add_events_batch(appeared_events)
                logger.info(
                    f"[DB] Inserted {len(appeared_events):,} appearance events in {time.time() - event_start:.2f}s"
                )

            previous_paths = set(self.current_state.keys())
            missing_paths = previous_paths - current_backup_paths

            disappeared_count = len(missing_paths)
            if missing_paths:
                disappear_start = time.time()
                missing_charter_ids = [
                    self.current_state[path] for path in missing_paths
                ]

                self.db.mark_charters_missing_batch(missing_charter_ids)

                disappeared_events = [
                    (charter_id, backup_id, "disappeared", backup_date_str)
                    for charter_id in missing_charter_ids
                ]
                self.db.add_events_batch(disappeared_events)

                for missing_path in missing_paths:
                    del self.current_state[missing_path]

                logger.info(
                    f"[DB] Processed {len(missing_paths):,} disappeared charters in {time.time() - disappear_start:.2f}s"
                )

            processing_time = time.time() - start_time
            charter_count = len(current_backup_paths)
            self.db.mark_backup_processed(backup_id, charter_count, processing_time)

            commit_start = time.time()
            self.db.conn.commit()
            commit_time = time.time() - commit_start
            logger.info(f"[DB] Committed transaction in {commit_time:.2f}s")
            logger.info(f"[DONE] Total processing time: {processing_time:.1f}s")

        except Exception:
            self.db.conn.rollback()
            raise

        return {
            "backup_id": backup_id,
            "filename": backup_filename,
            "date": backup_date_str,
            "charter_count": charter_count,
            "appeared": appeared_count,
            "disappeared": disappeared_count,
            "reappeared": reappeared_count,
            "discrepancies": len(discrepancies),
            "processing_time": processing_time,
        }
