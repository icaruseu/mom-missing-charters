"""MOM Missing Charters Tracker - CLI Entry Point."""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from src.azure_client import AzureBackupClient
from src.charter_tracker import CharterTracker
from src.database import Database
from src.utils import should_process_backup


def setup_logging(verbose: bool = False):
    """Configure logging.

    Args:
        verbose: Enable verbose (DEBUG) logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if not verbose:
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
            logging.WARNING
        )
        logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)
        logging.getLogger("azure").setLevel(logging.WARNING)


def load_config() -> dict:
    """Load configuration from environment variables.

    Returns:
        Configuration dictionary
    """
    load_dotenv()

    container_sas_url = os.getenv("AZURE_CONTAINER_SAS_URL")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

    if not container_sas_url and not (connection_string and container_name):
        print("Error: Must provide either:")
        print("  - AZURE_CONTAINER_SAS_URL")
        print("  OR")
        print("  - AZURE_STORAGE_CONNECTION_STRING + AZURE_CONTAINER_NAME")
        print("\nPlease create a .env file based on .env.example")
        sys.exit(1)

    return {
        "azure_container_sas_url": container_sas_url,
        "azure_connection_string": connection_string,
        "azure_container_name": container_name,
        "backup_cache_dir": os.getenv("BACKUP_CACHE_DIR", "./cache/backups"),
        "sqlite_db_path": os.getenv("SQLITE_DB_PATH", "./charters.db"),
        "reports_dir": os.getenv("REPORTS_DIR", "./reports"),
        "backup_frequency": int(os.getenv("BACKUP_FREQUENCY", "7")),
        "charter_base_path": os.getenv(
            "CHARTER_BASE_PATH", "db/mom-data/metadata.charter.public"
        ),
    }


def cmd_sync(args):
    """Sync and process backups."""
    setup_logging(verbose=args.verbose)
    config = load_config()

    print("MOM Missing Charters Tracker - Sync")
    print("=" * 60)

    # Initialize components
    azure_client = AzureBackupClient(
        cache_dir=config["backup_cache_dir"],
        connection_string=config["azure_connection_string"],
        container_name=config["azure_container_name"],
        container_sas_url=config["azure_container_sas_url"],
    )

    with Database(config["sqlite_db_path"]) as db:
        tracker = CharterTracker(db, config["charter_base_path"])
        tracker.load_current_state()

        print("\nFetching backup list from Azure...")
        all_backups = azure_client.list_full_backups()
        print(f"Found {len(all_backups)} full backups")

        frequency = config["backup_frequency"]
        backups_to_process = [
            backup
            for i, backup in enumerate(all_backups)
            if should_process_backup(i, frequency)
        ]

        if all_backups and all_backups[-1] not in backups_to_process:
            backups_to_process.append(all_backups[-1])

        print(
            f"Processing every {frequency} backup(s): {len(backups_to_process)} total (including latest)"
        )

        backups_to_process = [
            backup
            for backup in backups_to_process
            if not db.is_backup_processed(backup)
        ]

        if not backups_to_process:
            print("\nAll backups already processed!")
            return

        print(f"\n{len(backups_to_process)} new backup(s) to process\n")

        failed_backups = []

        for backup_filename in tqdm(backups_to_process, desc="Processing backups"):
            try:
                backup_path = azure_client.get_backup(backup_filename)
                stats = tracker.process_backup(backup_path, backup_filename)

                tqdm.write(
                    f"  {backup_filename}: "
                    f"{stats['charter_count']} charters, "
                    f"+{stats['appeared']} appeared, "
                    f"-{stats['disappeared']} disappeared, "
                    f"↻{stats['reappeared']} reappeared, "
                    f"⚠{stats['discrepancies']} discrepancies "
                    f"({stats['processing_time']:.1f}s)"
                )
            except Exception as e:
                tqdm.write(f"  ✗ {backup_filename}: ERROR - {e}")
                failed_backups.append((backup_filename, str(e)))
                continue

        if failed_backups:
            print(f"\n⚠ Warning: {len(failed_backups)} backup(s) failed to process:")
            for filename, error in failed_backups:
                print(f"  - {filename}: {error}")

    print("\nSync complete!")


def cmd_reset(args):
    """Reset database."""
    config = load_config()

    if not args.force:
        response = input("This will delete all charter tracking data. Continue? [y/N] ")
        if response.lower() != "y":
            print("Cancelled.")
            return

    with Database(config["sqlite_db_path"]) as db:
        db.reset()
        print("Database reset complete.")


def cmd_stats(args):
    """Show statistics."""
    config = load_config()

    with Database(config["sqlite_db_path"]) as db:
        stats = db.get_stats()

        print("\nMOM Missing Charters Tracker - Statistics")
        print("=" * 60)
        print(f"Processed backups:      {stats['processed_backups']:,}")
        print(f"Total charters:         {stats['total_charters']:,}")
        print(f"Missing charters:       {stats['missing_charters']:,}")
        print(f"Disappearance events:   {stats['disappearance_events']:,}")
        print(f"Total discrepancies:    {stats['total_discrepancies']:,}")
        print()


def cmd_report(args):
    """Generate missing charters report."""
    config = load_config()

    with Database(config["sqlite_db_path"]) as db:
        missing = db.get_missing_charters()

        if not missing:
            print("\nNo missing charters found!")
            return

        print(f"\nMissing Charters Report ({len(missing)} total)")
        print("=" * 60)

        if args.output:
            output_path = args.output
        elif args.save:
            # Auto-generate filename in reports directory
            reports_dir = Path(config["reports_dir"])
            reports_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            output_path = reports_dir / f"missing-charters-{timestamp}.csv"
        else:
            output_path = None

        if output_path:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=missing[0].keys())
                writer.writeheader()
                writer.writerows(missing)
            print(f"Report written to: {output_path}")
        else:
            for charter in missing[: args.limit]:
                print(f"\nPath: {charter['file_path']}")
                if charter.get("parent_path"):
                    print(f"  Parent: {charter['parent_path']}")
                print(
                    f"  First seen: {charter['first_seen_date']} ({charter['first_seen_backup']})"
                )
                print(
                    f"  Last seen:  {charter['last_seen_date']} ({charter['last_seen_backup']})"
                )

            if len(missing) > args.limit:
                print(f"\n... and {len(missing) - args.limit} more")
                print("Use --save or --output to save full report to CSV")


def cmd_parent_report(args):
    """Generate parent paths report showing missing charters by collection."""
    config = load_config()

    with Database(config["sqlite_db_path"]) as db:
        parent_stats = db.get_missing_charters_by_parent()

        if not parent_stats:
            print("\nNo missing charters found!")
            return

        total_missing = sum(p["missing_count"] for p in parent_stats)
        print(f"\nMissing Charters by Parent Path ({len(parent_stats)} collections, {total_missing} total charters)")
        print("=" * 60)

        if args.output:
            output_path = args.output
        elif args.save:
            # Auto-generate filename in reports directory
            reports_dir = Path(config["reports_dir"])
            reports_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            output_path = reports_dir / f"missing-by-parent-{timestamp}.csv"
        else:
            output_path = None

        if output_path:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=parent_stats[0].keys())
                writer.writeheader()
                writer.writerows(parent_stats)
            print(f"Report written to: {output_path}")
        else:
            for parent in parent_stats[: args.limit]:
                parent_path = parent["parent_path"] or "(root)"
                print(f"\n{parent_path}")
                print(f"  Missing charters: {parent['missing_count']}")
                print(f"  First seen range: {parent['earliest_first_seen']} to {parent['latest_first_seen']}")
                print(f"  Disappeared range: {parent['earliest_disappearance']} to {parent['latest_disappearance']}")

            if len(parent_stats) > args.limit:
                print(f"\n... and {len(parent_stats) - args.limit} more collections")
                print("Use --save or --output to save full report to CSV")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MOM Missing Charters Tracker - Track charter lifecycle across backups"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    sync_parser = subparsers.add_parser("sync", help="Download and process backups")
    sync_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    reset_parser = subparsers.add_parser("reset", help="Reset database")
    reset_parser.add_argument("--force", action="store_true", help="Skip confirmation")

    stats_parser = subparsers.add_parser("stats", help="Show statistics")

    report_parser = subparsers.add_parser(
        "report", help="Generate missing charters report"
    )
    report_parser.add_argument("--output", "-o", help="Output CSV file path")
    report_parser.add_argument(
        "--save", "-s", action="store_true", help="Save to reports directory with auto-generated filename"
    )
    report_parser.add_argument(
        "--limit", "-l", type=int, default=20, help="Limit console output"
    )

    parent_report_parser = subparsers.add_parser(
        "parent-report", help="Generate parent paths report (grouped by collection)"
    )
    parent_report_parser.add_argument("--output", "-o", help="Output CSV file path")
    parent_report_parser.add_argument(
        "--save", "-s", action="store_true", help="Save to reports directory with auto-generated filename"
    )
    parent_report_parser.add_argument(
        "--limit", "-l", type=int, default=20, help="Limit console output"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "sync":
        cmd_sync(args)
    elif args.command == "reset":
        cmd_reset(args)
    elif args.command == "stats":
        cmd_stats(args)
    elif args.command == "report":
        cmd_report(args)
    elif args.command == "parent-report":
        cmd_parent_report(args)


if __name__ == "__main__":
    main()
