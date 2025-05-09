import json
import os
import pathlib
from google.oauth2 import service_account
from googleapiclient.discovery import build  # type: ignore[import]
from googleapiclient.http import MediaFileUpload, HttpError  # type: ignore[import]
import argparse
import tempfile
import tarfile
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from typing import Any, Dict, List, Optional

from er_scarecrow_upload.common import init_application

DEFAULT_SERVICE_ACCOUNT_FILE = "/etc/er-scarecrow-upload/google-service-key.json"
DEFAULT_FOLDER_MAPPING = "/etc/er-scarecrow-upload/mapping.json"

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def is_retryable_http_error(exc: BaseException) -> bool:
    """
    Determine if an exception is a retryable HTTP error.

    Args:
        exc (BaseException): The exception to check.

    Returns:
        bool: True if the exception is retryable, False otherwise.
    """
    if isinstance(exc, HttpError):
        try:
            status = exc.resp.status  # e.g., 429, 500, etc.
        except Exception:
            return False
        return status in RETRYABLE_STATUS
    return False


class DriveService:
    def __init__(self, logger: Any, **kwargs: Any) -> None:
        """
        Initialize the DriveService with configuration and credentials.

        Args:
            logger (Any): Logger instance for logging messages.
            **kwargs (Any): Additional configuration options.
        """
        self.service_account_file: str = kwargs.get("service_account_file") or DEFAULT_SERVICE_ACCOUNT_FILE
        self.folder_mapping: str = kwargs.get("folder_mapping") or DEFAULT_FOLDER_MAPPING
        self.dry_run: bool = kwargs.get("dry_run", False)
        self.logger: Any = logger
        self.creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=["https://www.googleapis.com/auth/drive"]
        )
        self.drive = build("drive", "v3", credentials=self.creds)
        with open(self.folder_mapping) as f:
            self.folder_mapping = json.load(f)
        self.root_id: str = self.folder_mapping["root"]
        self.root_folder: Dict[str, str] = self.verify_shared_drive()
        self.drive_id: str = self.root_folder["driveId"]
        self.folder_cache: Dict[tuple, Dict[str, str]] = {}

    def verify_shared_drive(self) -> Dict[str, str]:
        """
        Verify the Shared Drive folder.

        Returns:
            Dict[str, str]: Metadata of the verified shared drive folder.
        """
        folder = self.drive.files().get(fileId=self.root_id, fields="id,name,driveId", supportsAllDrives=True).execute()
        self.logger.debug("✅ Shared Drive folder accessible", folder=folder["name"], id=folder["id"])
        return folder

    def get_drive_service(self) -> Any:
        """
        Get the Google Drive service instance.

        Returns:
            Any: Google Drive service instance.
        """
        return self.drive

    @retry(
        retry=retry_if_exception(is_retryable_http_error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=10),
        reraise=True,
    )
    def _call_create(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a file or folder in Google Drive.

        Args:
            **kwargs (Any): Parameters for the create API call.

        Returns:
            Dict[str, Any]: Metadata of the created file or folder.
        """
        if self.dry_run:
            self.logger.info("ℹ️  Dry run create", **kwargs)
            return {"id": "dry_run", "name": kwargs["body"]["name"]}
        return self.drive.files().create(**kwargs).execute()

    @retry(
        retry=retry_if_exception(is_retryable_http_error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=10),
        reraise=True,
    )
    def _call_update(self, efile: Dict[str, str], **kwargs: Any) -> Dict[str, Any]:
        """
        Update an existing file in Google Drive.

        Args:
            efile (Dict[str, str]): Metadata of the file to update.
            **kwargs (Any): Parameters for the update API call.

        Returns:
            Dict[str, Any]: Metadata of the updated file.
        """
        if self.dry_run:
            self.logger.info("ℹ️  Dry run update", **efile)
            return {"id": "dry_run", "name": kwargs["body"]["name"]}
        return self.drive.files().update(fileId=efile["id"], **kwargs).execute()

    def get_or_create_subfolders(self, parent: Dict[str, str], path: str, *paths: str) -> Dict[str, str]:
        """
        Get or create subfolders in Google Drive.

        Args:
            parent (Dict[str, str]): Metadata of the parent folder.
            path (str): Name of the first subfolder.
            *paths (str): Additional subfolder names.

        Returns:
            Dict[str, str]: Metadata of the last created or found subfolder.
        """
        parents = [parent]
        for name in (path, *paths):
            subfolder = self.get_subfolder(parents[-1]["id"], name)
            full_gdrive_path = f"{'/'.join(p['name'] for p in parents)}/{name}"
            if subfolder:
                folder_id = subfolder["id"]
                self.logger.debug("ℹ️ Found existing folder", name=full_gdrive_path, id=folder_id)
                parents.append(subfolder)
            else:
                dest_folder = self._call_create(
                    body={
                        "name": name,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [parents[-1]["id"]],
                    },
                    fields="id,name",
                    supportsAllDrives=True,
                )
                self.logger.info(
                    "✅ Created new folder",
                    name=full_gdrive_path,
                    id=dest_folder.get("id"),
                )
                parents.append(dest_folder)
        return parents[-1]

    def upload_hierarchy(
        self, local_root: pathlib.Path, folder: Dict[str, str], local_rel_directory: str = "."
    ) -> None:
        """
        Upload a directory hierarchy to Google Drive.

        Args:
            local_root (pathlib.Path): Local root directory.
            folder (Dict[str, str]): Metadata of the destination folder in Google Drive.
            local_rel_directory (str): Relative path to the local directory to upload.
        """
        local_root = pathlib.Path(local_root)
        local_rel_directory = pathlib.Path(local_rel_directory)
        to_upload = local_root / local_rel_directory
        for root, dirs, files in os.walk(to_upload):
            for file in files:
                parent = self.get_or_create_subfolders(folder, *pathlib.Path(root).relative_to(local_root).parts)
                uploaded_file = self.create_or_update_file(parent, pathlib.Path(root) / file)
                self.logger.debug("Uploaded file", name=str(pathlib.Path(root) / file), id=uploaded_file["id"])

    def upload_archive(self, archive_file: pathlib.Path, folder: Dict[str, str]) -> None:
        """
        Extract and upload an archive file to Google Drive.

        Args:
            archive_file (pathlib.Path): Path to the archive file.
            folder (Dict[str, str]): Metadata of the destination folder in Google Drive.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = pathlib.Path(temp_dir)
            with tarfile.open(archive_file) as tf:
                tf.extractall(path=temp_dir_path)
            self.upload_hierarchy(temp_dir_path, folder)

    @retry(
        retry=retry_if_exception(is_retryable_http_error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _drive_list(self, **kwargs: Any) -> List[Dict[str, str]]:
        """
        List files or folders in Google Drive.

        Args:
            **kwargs (Any): Parameters for the list API call.

        Returns:
            List[Dict[str, str]]: List of files or folders.
        """
        return (
            self.drive.files()
            .list(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="drive",
                spaces="drive",
                driveId=self.drive_id,
                **kwargs,
            )
            .execute()
            .get("files", [])
        )

    def get_subfolder(self, folder_id: str, name: str) -> Optional[Dict[str, str]]:
        """
        Get a subfolder by name in a parent folder.

        Args:
            folder_id (str): ID of the parent folder.
            name (str): Name of the subfolder.

        Returns:
            Optional[Dict[str, str]]: Metadata of the subfolder if found, None otherwise.
        """
        if (folder_id, name) in self.folder_cache:
            return self.folder_cache[(folder_id, name)]
        query_template = (
            "mimeType='application/vnd.google-apps.folder' "
            "and name='{name}' "
            "and '{parent}' in parents "
            "and trashed=false"
        )

        items = self._drive_list(
            q=query_template.format(name=name, parent=folder_id),
            fields="files(id, name)",
            pageSize=1,
        )
        if items:
            self.folder_cache[(folder_id, name)] = items[0]
            return items[0]
        return None

    def create_or_update_file(self, parent: Dict[str, str], local_path: pathlib.Path) -> Dict[str, Any]:
        """
        Create or update a file in Google Drive.

        Args:
            parent (Dict[str, str]): Metadata of the parent folder.
            local_path (pathlib.Path): Path to the local file.

        Returns:
            Dict[str, Any]: Metadata of the created or updated file.
        """
        local_path = pathlib.Path(local_path)
        dfile = self.get_file(parent, local_path.name)
        media = MediaFileUpload(local_path, resumable=True)
        if dfile:
            return self._call_update(
                dfile,
                fields="id,name",
                media_body=media,
                supportsAllDrives=True,
            )
        file_metadata = {"name": local_path.name, "parents": [parent["id"]]}
        return self._call_create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )

    def get_file(self, parent: Dict[str, str], filename: str) -> Optional[Dict[str, str]]:
        """
        Get a file by name in a parent folder.

        Args:
            parent (Dict[str, str]): Metadata of the parent folder.
            filename (str): Name of the file.

        Returns:
            Optional[Dict[str, str]]: Metadata of the file if found, None otherwise.
        """
        query = f"name = '{filename}' and trashed = false and '{parent['id']}' in parents"
        files = self._drive_list(
            q=query,
            fields="files(id, name)",
            pageSize=1,
        )
        return files[0] if files else None


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Upload a file to a Google Drive folder using a service account.")
    args, logger = init_application(
        "er-scarecrow-upload",
        "Upload files to Google Drive",
        get_parser,
    )
    service = DriveService(logger, **vars(args))
    if args.check:
        return
    if args.upload:
        if args.upload_archive:
            dest = service.get_or_create_subfolders(service.root_folder, *pathlib.Path(args.upload_directory).parts)
            service.upload_archive(args.upload_archive, dest)
        elif args.upload_directory:
            dest = service.get_or_create_subfolders(service.root_folder, *pathlib.Path(args.upload_directory).parts)
            service.upload_hierarchy(args.upload_root, dest, args.upload_local_directory)
        else:
            parser.error("Either --upload-archive or --upload-directory must be specified.")


def get_parser(parser: argparse.ArgumentParser):

    parser.add_argument(
        "-s",
        "--service-account-file",
        type=str,
        help=f"Path to the service account JSON key file.(default:{DEFAULT_SERVICE_ACCOUNT_FILE})",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Do not upload files, just check the folder ID.",
        default=False,
    )
    parser.add_argument(
        "-m",
        "--folder-mapping",
        type=str,
        help=f"path to a json file containing the root folder mappings.(default:{DEFAULT_FOLDER_MAPPING})",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file-name", type=str, nargs="+", help="Name of the file(s) to upload.")
    group.add_argument(
        "--check",
        action="store_true",
        help="Check the folder ID without uploading files.",
        default=False,
    )
    group.add_argument("--upload", action="store_true", help="Upload files.", default=False)
    upload_group = parser.add_argument_group("Upload options")
    upload_variants = upload_group.add_mutually_exclusive_group()
    upload_variants.add_argument("--upload-archive", type=pathlib.Path, help="Path to the file to upload.")
    upload_variants.add_argument(
        "--upload-directory",
        type=str,
        help="Path name of to the GDrive to upload the files into.",
    )
    upload_group.add_argument(
        "--upload-root",
        type=str,
        help="Path to the root directory for hiearchical upload.",
        default=".",
    )
    upload_group.add_argument(
        "--upload-local-directory",
        type=str,
        help="relative path to the local upload directory to upload.",
        default=".",
    )
    upload_group.add_argument(
        "--upload-cleanup",
        action="store_true",
        help="Cleanup the upload artifacts after successful upload.",
        default=False,
    )
    return parser


if __name__ == "__main__":
    main()
