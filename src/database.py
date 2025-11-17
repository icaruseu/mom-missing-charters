"""Database operations for charter lifecycle tracking."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import final


@final
class Database:
    """SQLite database manager for charter tracking."""

    def __init__(self, db_path: str):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None

    def connect(self):
        """Connect to database and create tables if needed."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA cache_size=-64000")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        self._create_tables()
        self._create_indexes()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _create_tables(self):
        """Create database tables."""
        cursor = self.conn.cursor()

        # Backups table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                backup_date TEXT NOT NULL,
                processed_at TEXT,
                charter_count INTEGER DEFAULT 0,
                processing_time_sec REAL,
                UNIQUE(filename)
            )
        """)

        # Charters table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS charters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_path_raw TEXT,
                first_seen_backup_id INTEGER,
                last_seen_backup_id INTEGER,
                current_status TEXT DEFAULT 'present',
                FOREIGN KEY (first_seen_backup_id) REFERENCES backups(id),
                FOREIGN KEY (last_seen_backup_id) REFERENCES backups(id),
                UNIQUE(file_path)
            )
        """)

        # Charter events table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS charter_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charter_id INTEGER NOT NULL,
                backup_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_date TEXT NOT NULL,
                FOREIGN KEY (charter_id) REFERENCES charters(id),
                FOREIGN KEY (backup_id) REFERENCES backups(id)
            )
        """)

        # Discrepancies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discrepancies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                in_contents_xml BOOLEAN NOT NULL,
                in_zip_entries BOOLEAN NOT NULL,
                FOREIGN KEY (backup_id) REFERENCES backups(id)
            )
        """)

        self.conn.commit()

    def _create_indexes(self):
        """Create database indexes for performance."""
        cursor = self.conn.cursor()

        # Indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_charters_path
            ON charters(file_path)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_charters_status
            ON charters(current_status)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_charter
            ON charter_events(charter_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_backup
            ON charter_events(backup_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_type
            ON charter_events(event_type)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_backups_date
            ON backups(backup_date)
        """)

        self.conn.commit()

    def reset(self):
        """Reset database by dropping all tables."""
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS discrepancies")
        cursor.execute("DROP TABLE IF EXISTS charter_events")
        cursor.execute("DROP TABLE IF EXISTS charters")
        cursor.execute("DROP TABLE IF EXISTS backups")
        self.conn.commit()
        self._create_tables()
        self._create_indexes()

    def add_backup(self, filename: str, backup_date: str) -> int:
        """Add a backup record.

        Args:
            filename: Backup filename
            backup_date: ISO format date

        Returns:
            Backup ID
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO backups (filename, backup_date)
            VALUES (?, ?)
        """,
            (filename, backup_date),
        )
        self.conn.commit()

        cursor.execute("SELECT id FROM backups WHERE filename = ?", (filename,))
        return cursor.fetchone()[0]

    def mark_backup_processed(
        self, backup_id: int, charter_count: int, processing_time: float
    ):
        """Mark a backup as processed.

        Args:
            backup_id: Backup ID
            charter_count: Number of charters found
            processing_time: Processing time in seconds
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE backups
            SET processed_at = ?, charter_count = ?, processing_time_sec = ?
            WHERE id = ?
        """,
            (datetime.now().isoformat(), charter_count, processing_time, backup_id),
        )
        self.conn.commit()

    def is_backup_processed(self, filename: str) -> bool:
        """Check if a backup has been processed.

        Args:
            filename: Backup filename

        Returns:
            True if processed, False otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT processed_at FROM backups WHERE filename = ? AND processed_at IS NOT NULL
        """,
            (filename,),
        )
        return cursor.fetchone() is not None

    def get_charter_by_path(self, file_path: str) -> dict | None:
        """Get charter by normalized path.

        Args:
            file_path: Normalized charter path

        Returns:
            Charter record or None
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM charters WHERE file_path = ?", (file_path,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def add_charter(self, file_path: str, file_path_raw: str, backup_id: int) -> int:
        """Add a new charter.

        Args:
            file_path: Normalized charter path
            file_path_raw: Raw charter path from backup
            backup_id: Backup where first seen

        Returns:
            Charter ID
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO charters
            (file_path, file_path_raw, first_seen_backup_id, last_seen_backup_id, current_status)
            VALUES (?, ?, ?, ?, 'present')
        """,
            (file_path, file_path_raw, backup_id, backup_id),
        )
        return cursor.lastrowid

    def add_charters_batch(self, charters: list[tuple[str, str, int]]) -> list[int]:
        """Add multiple charters in a batch.

        Args:
            charters: List of (file_path, file_path_raw, backup_id) tuples

        Returns:
            List of charter IDs in same order as input
        """
        if not charters:
            return []

        cursor = self.conn.cursor()

        values = [(fp, fpr, bid, bid, "present") for fp, fpr, bid in charters]
        cursor.executemany(
            """
            INSERT INTO charters
            (file_path, file_path_raw, first_seen_backup_id, last_seen_backup_id, current_status)
            VALUES (?, ?, ?, ?, ?)
        """,
            values,
        )

        CHUNK_SIZE = 900
        paths = [charter[0] for charter in charters]
        path_to_id = {}

        for i in range(0, len(paths), CHUNK_SIZE):
            chunk = paths[i : i + CHUNK_SIZE]
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(
                f"""
                SELECT id, file_path FROM charters
                WHERE file_path IN ({placeholders})
            """,
                chunk,
            )

            for row in cursor.fetchall():
                path_to_id[row[1]] = row[0]

        return [path_to_id[charter[0]] for charter in charters]

    def update_charter_last_seen(self, charter_id: int, backup_id: int):
        """Update charter's last seen backup.

        Args:
            charter_id: Charter ID
            backup_id: Backup ID
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE charters
            SET last_seen_backup_id = ?, current_status = 'present'
            WHERE id = ?
        """,
            (backup_id, charter_id),
        )

    def update_charters_last_seen_batch(self, charter_ids: list[int], backup_id: int):
        """Update multiple charters' last seen backup.

        Args:
            charter_ids: List of charter IDs
            backup_id: Backup ID
        """
        if not charter_ids:
            return

        cursor = self.conn.cursor()
        values = [(backup_id, charter_id) for charter_id in charter_ids]
        cursor.executemany(
            """
            UPDATE charters
            SET last_seen_backup_id = ?, current_status = 'present'
            WHERE id = ?
        """,
            values,
        )

    def mark_charter_missing(self, charter_id: int):
        """Mark charter as missing.

        Args:
            charter_id: Charter ID
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE charters
            SET current_status = 'missing'
            WHERE id = ?
        """,
            (charter_id,),
        )

    def mark_charters_missing_batch(self, charter_ids: list[int]):
        """Mark multiple charters as missing.

        Args:
            charter_ids: List of charter IDs
        """
        if not charter_ids:
            return

        cursor = self.conn.cursor()
        CHUNK_SIZE = 900

        for i in range(0, len(charter_ids), CHUNK_SIZE):
            chunk = charter_ids[i : i + CHUNK_SIZE]
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(
                f"""
                UPDATE charters
                SET current_status = 'missing'
                WHERE id IN ({placeholders})
            """,
                chunk,
            )

    def add_event(
        self, charter_id: int, backup_id: int, event_type: str, event_date: str
    ):
        """Add a charter event.

        Args:
            charter_id: Charter ID
            backup_id: Backup ID
            event_type: 'appeared' or 'disappeared'
            event_date: ISO format date
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO charter_events (charter_id, backup_id, event_type, event_date)
            VALUES (?, ?, ?, ?)
        """,
            (charter_id, backup_id, event_type, event_date),
        )

    def add_events_batch(self, events: list[tuple[int, int, str, str]]):
        """Add multiple charter events in a batch.

        Args:
            events: List of (charter_id, backup_id, event_type, event_date) tuples
        """
        if not events:
            return

        cursor = self.conn.cursor()
        cursor.executemany(
            """
            INSERT INTO charter_events (charter_id, backup_id, event_type, event_date)
            VALUES (?, ?, ?, ?)
        """,
            events,
        )

    def add_discrepancy(
        self,
        backup_id: int,
        file_path: str,
        in_contents_xml: bool,
        in_zip_entries: bool,
    ):
        """Add a discrepancy record.

        Args:
            backup_id: Backup ID
            file_path: Charter path
            in_contents_xml: Present in __contents__.xml
            in_zip_entries: Present in actual ZIP entries
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO discrepancies
            (backup_id, file_path, in_contents_xml, in_zip_entries)
            VALUES (?, ?, ?, ?)
        """,
            (backup_id, file_path, in_contents_xml, in_zip_entries),
        )

    def add_discrepancies_batch(self, discrepancies: list[tuple[int, str, bool, bool]]):
        """Add multiple discrepancy records in a batch.

        Args:
            discrepancies: List of (backup_id, file_path, in_contents_xml, in_zip_entries) tuples
        """
        if not discrepancies:
            return

        cursor = self.conn.cursor()
        cursor.executemany(
            """
            INSERT INTO discrepancies
            (backup_id, file_path, in_contents_xml, in_zip_entries)
            VALUES (?, ?, ?, ?)
        """,
            discrepancies,
        )

    def get_all_charters_for_backup(self, backup_id: int) -> set:
        """Get all charter paths present in a specific backup.

        Args:
            backup_id: Backup ID

        Returns:
            Set of normalized charter paths
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT c.file_path
            FROM charters c
            WHERE c.first_seen_backup_id <= ?
              AND (c.last_seen_backup_id >= ? OR c.current_status = 'present')
        """,
            (backup_id, backup_id),
        )
        return {row[0] for row in cursor.fetchall()}

    def get_charters_by_paths_batch(self, file_paths: list[str]) -> dict[str, dict]:
        """Get multiple charters by their normalized paths.

        Args:
            file_paths: List of normalized charter paths

        Returns:
            Dictionary mapping file_path to charter record
        """
        if not file_paths:
            return {}

        CHUNK_SIZE = 900
        result = {}
        cursor = self.conn.cursor()

        for i in range(0, len(file_paths), CHUNK_SIZE):
            chunk = file_paths[i : i + CHUNK_SIZE]
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(
                f"""
                SELECT * FROM charters
                WHERE file_path IN ({placeholders})
            """,
                chunk,
            )

            for row in cursor.fetchall():
                result[row["file_path"]] = dict(row)

        return result

    def get_stats(self) -> dict:
        """Get database statistics.

        Returns:
            Dictionary with statistics
        """
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM backups WHERE processed_at IS NOT NULL")
        processed_backups = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM charters")
        total_charters = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM charters WHERE current_status = 'missing'")
        missing_charters = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM charter_events WHERE event_type = 'disappeared'"
        )
        disappearance_events = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM discrepancies")
        total_discrepancies = cursor.fetchone()[0]

        return {
            "processed_backups": processed_backups,
            "total_charters": total_charters,
            "missing_charters": missing_charters,
            "disappearance_events": disappearance_events,
            "total_discrepancies": total_discrepancies,
        }

    def get_missing_charters(self) -> list:
        """Get all missing charters with details.

        Returns:
            List of missing charter records
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                c.file_path,
                c.file_path_raw,
                b1.filename as first_seen_backup,
                b1.backup_date as first_seen_date,
                b2.filename as last_seen_backup,
                b2.backup_date as last_seen_date
            FROM charters c
            LEFT JOIN backups b1 ON c.first_seen_backup_id = b1.id
            LEFT JOIN backups b2 ON c.last_seen_backup_id = b2.id
            WHERE c.current_status = 'missing'
            ORDER BY b2.backup_date DESC
        """)
        return [dict(row) for row in cursor.fetchall()]

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
