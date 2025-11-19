# MOM Missing Charters Tracker

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

# Sync and process backups
sync:
    uv run main.py sync

# Sync with verbose logging
sync-verbose:
    uv run main.py sync --verbose

# Show statistics
stats:
    uv run main.py stats

# Generate and save missing charters report to CSV
report-save:
    uv run main.py report --save

# Save parent paths report to CSV
parent-report-save:
    uv run main.py parent-report --save

# Extract missing charters to ZIP file (auto-generated filename)
extract-missing:
    uv run main.py extract-missing

# Extract missing charters and save failed items log
extract-missing-with-log:
    uv run main.py extract-missing --save-failed

# Generate all reports
reports: report-save parent-report-save
    @echo "All reports generated in reports/ directory"
