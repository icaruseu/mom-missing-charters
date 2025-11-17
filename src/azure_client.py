"""Azure Blob Storage client with local caching."""

from pathlib import Path
from typing import final
from zipfile import BadZipFile, ZipFile

from azure.storage.blob import BlobServiceClient, ContainerClient
from tqdm import tqdm


@final
class AzureBackupClient:
    """Azure Blob Storage client for backup files with local caching."""

    def __init__(
        self,
        cache_dir: str,
        connection_string: str | None = None,
        container_name: str | None = None,
        container_sas_url: str | None = None,
    ):
        """Initialize Azure client.

        Args:
            cache_dir: Local directory for caching downloaded backups
            connection_string: Azure Storage connection string (if using connection string auth)
            container_name: Container name (required with connection_string)
            container_sas_url: Full container SAS URL (alternative to connection_string)

        Raises:
            ValueError: If neither connection_string+container_name nor container_sas_url is provided
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if container_sas_url:
            self.container_client = ContainerClient.from_container_url(
                container_sas_url
            )
            self.blob_service_client = None
            self.container_name = None
        elif connection_string and container_name:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            self.container_client = self.blob_service_client.get_container_client(
                container_name
            )
            self.container_name = container_name
        else:
            raise ValueError(
                "Must provide either 'container_sas_url' OR both 'connection_string' and 'container_name'"
            )

    def list_full_backups(self) -> list[str]:
        """List all full backup files in the container.

        Returns:
            Sorted list of backup filenames (chronologically)
        """
        blobs = self.container_client.list_blobs(name_starts_with="full")
        backup_files = [
            blob.name
            for blob in blobs
            if blob.name.endswith(".zip") and blob.name.startswith("full")
        ]
        return sorted(backup_files)

    def get_backup(
        self, filename: str, force_download: bool = False, max_retries: int = 3
    ) -> Path:
        """Get backup file, downloading from Azure if not cached.

        Args:
            filename: Backup filename
            force_download: Force re-download even if cached
            max_retries: Maximum number of download attempts if validation fails

        Returns:
            Path to local backup file

        Raises:
            BadZipFile: If file is not a valid ZIP after max_retries attempts
        """
        cache_path = self.cache_dir / filename

        if cache_path.exists() and not force_download:
            if cache_path.stat().st_size > 0 and self._is_valid_zip(cache_path):
                return cache_path
            else:
                print(
                    f"  Warning: Cached file {filename} is corrupted, re-downloading..."
                )
                cache_path.unlink()

        for attempt in range(max_retries):
            try:
                self._download_backup(filename, cache_path)

                if self._is_valid_zip(cache_path):
                    return cache_path
                else:
                    cache_path.unlink()
                    if attempt < max_retries - 1:
                        print(
                            f"  Warning: Downloaded file is not a valid ZIP, retrying ({attempt + 1}/{max_retries})..."
                        )
            except Exception as e:
                if cache_path.exists():
                    cache_path.unlink()
                if attempt < max_retries - 1:
                    print(
                        f"  Warning: Download failed ({e}), retrying ({attempt + 1}/{max_retries})..."
                    )
                else:
                    raise

        raise BadZipFile(
            f"Failed to download valid ZIP file after {max_retries} attempts: {filename}"
        )

    def _is_valid_zip(self, file_path: Path) -> bool:
        """Check if a file is a valid ZIP file.

        Args:
            file_path: Path to file to validate

        Returns:
            True if file is a valid ZIP, False otherwise
        """
        try:
            with ZipFile(file_path, "r") as zf:
                _ = zf.namelist()
            return True
        except (BadZipFile, OSError, EOFError):
            return False

    def _download_backup(self, filename: str, dest_path: Path):
        """Download backup from Azure with progress bar.

        Args:
            filename: Backup filename in Azure
            dest_path: Destination path for downloaded file
        """
        blob_client = self.container_client.get_blob_client(filename)
        properties = blob_client.get_blob_properties()
        blob_size = properties.size

        with dest_path.open("wb") as f:
            with tqdm(
                total=blob_size,
                unit="B",
                unit_scale=True,
                desc=f"Downloading {filename}",
                leave=False,
            ) as pbar:
                stream = blob_client.download_blob()
                for chunk in stream.chunks():
                    f.write(chunk)
                    pbar.update(len(chunk))

    def get_backup_size(self, filename: str) -> int:
        """Get backup file size in bytes.

        Args:
            filename: Backup filename

        Returns:
            File size in bytes
        """
        cache_path = self.cache_dir / filename
        if cache_path.exists():
            return cache_path.stat().st_size

        blob_client = self.container_client.get_blob_client(filename)
        properties = blob_client.get_blob_properties()
        return properties.size

    def clear_cache(self):
        """Clear all cached backup files."""
        for cache_file in self.cache_dir.glob("*.zip"):
            cache_file.unlink()
