# MOM Missing Charters Tracker

Tracks charter document lifecycle across historical backups of monasterium.net to identify missing documents.

## What It Does

Analyzes ~400 eXist-db backup files (2017-present) containing 700K-1M charter XML files to detect:

- When charters first appeared
- When charters disappeared
- When charters reappeared (if applicable)
- Discrepancies between backup manifests and actual files

## How It Works

1. **Downloads backups** from Azure Blob Storage (with local caching)
2. **Extracts charter paths** from both `__contents__.xml` manifests and actual ZIP entries
3. **Tracks state changes** in SQLite database (appeared/disappeared events)
4. **Normalizes paths** to handle Unicode and URL encoding variants
5. **Processes chronologically** to build accurate timeline

## Setup

1. Install dependencies:

   ```bash
   uv sync
   ```

2. Configure `.env` (copy from `.env.example`):
   ```bash
   AZURE_CONTAINER_SAS_URL=https://account.blob.core.windows.net/container?sp=rl&st=...
   BACKUP_FREQUENCY=7  # Process every Nth backup (1=all, 7=weekly sample)
   ```

## Usage

```bash
# Process backups and track charters
python main.py sync [--verbose] [--start-date YYYY-MM-DD]

# Show statistics
python main.py stats

# Generate reports
python main.py report [--save | --output PATH] [--limit N]
python main.py parent-report [--save | --output PATH] [--limit N]

# Extract missing charter files into ZIP
python main.py extract-missing [--output PATH] [--save-failed] [--verbose]

# Reset database
python main.py reset [--force]
```

### Commands

- **sync**: Process backups chronologically and track state changes
  - `--verbose`: Enable debug logging
  - `--start-date YYYY-MM-DD`: Set new baseline - first backup after this date becomes the starting point
    - Useful when corrections were made on a date: since backups are nightly, the backup from that date was likely made before corrections
    - Excludes the specified date and all earlier backups
  - Always processes latest backup regardless of frequency setting

- **stats**: Show summary statistics (total charters, missing, events, discrepancies)

- **report**: List all missing charters with timeline (first seen, last seen dates)
  - `--save`: Auto-generate timestamped CSV in reports directory
  - `--output PATH`: Save to specific CSV file
  - `--limit N`: Limit console output (default: 20)

- **parent-report**: Group missing charters by collection/parent path for pattern analysis
  - Same options as `report` command

- **extract-missing**: Extract actual charter XML files from last-seen backups
  - Handles URL encoding and Unicode normalization via path variant matching
  - Processes all missing charters across multiple backup files
  - `--output PATH`: Specify output ZIP path (default: auto-generated)
  - `--save-failed`: Generate CSV log of extraction failures
  - `--verbose`: Enable debug logging

- **reset**: Clear all tracking data from database
  - `--force`: Skip confirmation prompt

## Configuration

### Environment Variables

Configure in `.env`:

- `AZURE_CONTAINER_SAS_URL`: Container SAS URL (recommended)
  - Alternative: `AZURE_STORAGE_CONNECTION_STRING` + `AZURE_CONTAINER_NAME`
- `BACKUP_FREQUENCY`: Sampling rate - process every Nth backup (default: 7)
  - `1` = all backups, `7` = every 7th, `30` = every 30th
  - Latest backup always included regardless of this setting
- `START_DATE`: Set new baseline date in YYYY-MM-DD format (optional)
  - First backup after this date becomes the new baseline for tracking
  - Use when corrections were made on a date: backups from that date were likely made before corrections (nightly backups)
  - Prevents false positives from before the correction date
  - Command-line `--start-date` overrides this setting
  - Example: `START_DATE=2025-12-05` (first backup after 2025-12-05 becomes new baseline)
- `BACKUP_CACHE_DIR`: Local cache for downloaded backups (default: `./cache/backups`)
- `SQLITE_DB_PATH`: Database location (default: `./charters.db`)
- `REPORTS_DIR`: Report output directory (default: `./reports`)
- `CHARTER_BASE_PATH`: Charter collection path in backups (default: `db/mom-data/metadata.charter.public`)

### Ignored Parent Paths

Create `ignored_parent_paths.txt` to exclude specific parent paths from reports and extraction. Useful for charters that were intentionally deleted or renamed.

Format:

```
# Comments start with #
# One parent path per line
Fontenay
CZ-NA/AZK|Anezka
IlluminierteUrkunden
```

Affects: `stats`, `report`, `parent-report`, and `extract-missing` commands

## Technical Details

### Architecture

- **Storage**: Azure Blob → Local cache → SQLite database
- **Scale**: 700K-1M charters across ~400 backups
- **Processing**: Chronological backup processing for accurate lifecycle tracking
- **Efficiency**: State-change tracking only (not every backup for every charter)

### Backup Format

- **Naming**: `fullYYYYMMDD-HHMM.zip` (e.g., `full20170815-0400.zip`)
- **Contents**: Standard eXist-db ZIP backups with `__contents__.xml` manifest
- **Critical issue**: `__contents__.xml` manifests are sometimes incomplete or incorrect
- **Solution**: Always cross-reference manifest with actual ZIP entries

### Unicode & Path Handling

Charter filenames contain complex Unicode characters that may appear in different forms:

- **URL-encoded**: `%C3%A4` (escaped)
- **Unescaped**: `ä` (literal Unicode)
- **Normalization**: All paths normalized to Unicode NFC form for reliable comparison
- **Storage**: Both normalized (for comparison) and raw (for debugging) paths stored
- **Extraction**: Path variant generation handles encoding differences during file retrieval

### Design Principles

1. **Don't trust manifests**: Always cross-reference `__contents__.xml` with actual ZIP entries
2. **Normalize paths**: Handle Unicode and URL encoding variants for reliable comparison
3. **Track changes only**: Don't record every backup for every charter (efficiency)
4. **Cache aggressively**: Minimize Azure bandwidth and re-processing time
5. **Process chronologically**: Essential for correct appearance/disappearance sequencing
6. **Always include latest**: Most recent backup always processed regardless of frequency setting
7. **Batch operations**: Handle 700K-1M charter scale efficiently with bulk inserts and transactions
