# MOM Missing Charters Tracker - Justfile
# Run 'just' or 'just --list' to see all available commands

# Default recipe - show help
default:
    @just --list

# Install dependencies using uv
install:
    uv sync

# Setup environment (create .env from example if needed)
setup:
    @if [ ! -f .env ]; then \
        echo "Creating .env from .env.example..."; \
        cp .env.example .env; \
        echo "Please edit .env with your Azure credentials"; \
    else \
        echo ".env already exists"; \
    fi

# === Main Commands ===

# Sync and process backups (normal mode, every 7th backup)
sync:
    uv run main.py sync

# Sync with verbose logging
sync-verbose:
    uv run main.py sync --verbose

# Show statistics
stats:
    uv run main.py stats

# Reset database (with confirmation)
reset:
    uv run main.py reset

# Reset database (skip confirmation)
reset-force:
    uv run main.py reset --force

# === Reports ===

# Generate missing charters report (console output, 20 items)
report:
    uv run main.py report

# Generate and save missing charters report to CSV
report-save:
    uv run main.py report --save

# Generate missing charters report with custom limit
report-limit limit="50":
    uv run main.py report --limit {{limit}}

# Generate missing charters report to specific file
report-to file:
    uv run main.py report --output {{file}}

# Generate parent paths report (grouped by collection)
parent-report:
    uv run main.py parent-report

# Save parent paths report to CSV
parent-report-save:
    uv run main.py parent-report --save

# === Database Management ===

# Open SQLite database in interactive shell
db-shell:
    sqlite3 charters.db

# Show database schema
db-schema:
    sqlite3 charters.db ".schema"

# Show table sizes
db-tables:
    @echo "Database table row counts:"
    @sqlite3 charters.db "SELECT 'backups', COUNT(*) FROM backups UNION ALL SELECT 'charters', COUNT(*) FROM charters UNION ALL SELECT 'charter_events', COUNT(*) FROM charter_events UNION ALL SELECT 'discrepancies', COUNT(*) FROM discrepancies;"

# Vacuum database (reclaim space after deletions)
db-vacuum:
    sqlite3 charters.db "VACUUM;"

# === Cache Management ===

# Show cache directory size
cache-size:
    @echo "Backup cache size:"
    @du -sh cache/backups 2>/dev/null || echo "Cache directory empty or doesn't exist"

# List cached backups
cache-list:
    @echo "Cached backups:"
    @ls -lh cache/backups/*.zip 2>/dev/null | wc -l | xargs echo "Total files:"
    @ls -lh cache/backups/*.zip 2>/dev/null | tail -10 || echo "No cached backups found"

# Clear backup cache
cache-clear:
    @read -p "Delete all cached backups? [y/N] " confirm && \
    if [ "$$confirm" = "y" ]; then \
        rm -rf cache/backups/*; \
        echo "Cache cleared"; \
    else \
        echo "Cancelled"; \
    fi

# === Development ===

# Format code with ruff
format:
    uv run ruff format .

# Lint code with ruff
lint:
    uv run ruff check .

# Type check with mypy (if configured)
typecheck:
    uv run mypy src/ main.py

# Run all checks (format, lint, typecheck)
check: format lint

# === Full Workflow ===

# Complete fresh start: reset DB, clear cache, sync all
fresh: reset-force
    rm -rf cache/backups/*
    @echo "Starting fresh sync..."
    just sync

# Quick workflow: sync and show stats
quick: sync stats

# Generate all reports
reports: report-save parent-report-save
    @echo "All reports generated in reports/ directory"
