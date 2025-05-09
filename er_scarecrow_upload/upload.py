import os
import pathlib
from google.oauth2 import service_account
from googleapiclient.discovery import build  # type: ignore[import]
from googleapiclient.http import MediaFileUpload, HttpError  # type: ignore[import]
import argparse
import subprocess
import tempfile

from er_scarecrow_upload.common import init_application


class DriveService:
    def __init__(self, service_account_file, logger, arsg: argparse.Namespace):
        self.args = arsg
        self.logger = logger
        self.creds = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=["https://www.googleapis.com/auth/drive"]
        )
        self.drive = build("drive", "v3", credentials=self.creds)

    def verify_shared_drive(self, folder_id):
        # Verify the Shared Drive folder
        folder = self.drive.files().get(fileId=folder_id, fields="id,name", supportsAllDrives=True).execute()
        self.logger.info("✅ Shared Drive folder:", folder=folder["name"], id=folder["id"])
        return folder

    def get_drive_service(self):
        return self.drive

    def _call_create(self, **kwargs):
        if self.args.dry_run:
            self.logger.info("ℹ️  Dry run create", **kwargs)
            return {"id": "dry_run", "name": kwargs["body"]["name"]}
        return self.drive.files().create(**kwargs).execute()

    def get_or_create_subfolders(self, parent, path, *paths):
        parents = [parent]
        for name in (path, *paths):
            # 2) Look for an existing folder with that name
            subfolder = self.get_subfolder(parent["id"], name)
            full_gdrive_path = f"{'/'.join(p['name'] for p in parents)}/{name}"
            if subfolder:
                folder_id = subfolder["id"]
                self.logger.debug("ℹ️ Found existing folder", name=full_gdrive_path, id=folder_id)
                parents.append(subfolder)
            else:
                # 3) Create the folder since it doesn’t exist
                dest_folder = self._call_create(
                    body={
                        "name": name,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [parent["id"]],
                    },
                    fields="id",
                    supportsAllDrives=True,
                )
                self.logger.info(
                    "✅ Created new folder",
                    name=full_gdrive_path,
                    id=dest_folder.get("id"),
                )
                parents.append(dest_folder)
            return parents[-1]

    def upload_hierachy(self, local_root, folder, local_rel_directory="."):
        local_root = pathlib.Path(local_root)
        local_rel_directory = pathlib.Path(local_rel_directory)
        to_upload = local_root / local_rel_directory
        for root, dirs, files in os.walk(to_upload):
            for d in dirs:
                upload_dir = pathlib.Path(root) / d
                upload_dir.relative_to(local_root).parts
                self.get_or_create_subfolders()
            for file in files:
                media = MediaFileUpload(pathlib.Path(root) / file, resumable=True)
                file_metadata = {"name": file, "parents": [folder["id"]]}
                new_file = self._call_create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                self.logger.debug("Uploaded file", name=file, id=new_file["id"])

    def upload_archive(self, archive_file: pathlib.Path, folder):
        temp_dir = tempfile.mkdtemp()
        # Create a temporary directory
        temp_dir_path = pathlib.Path(temp_dir)
        # Copy the archive file to the temporary directory
        temp_archive_file = temp_dir_path / archive_file.name
        subprocess.check_call(["cp", str(archive_file), str(temp_archive_file)])
        subprocess.check_call(["tar", "-C", str(temp_dir_path), "-xf", str(temp_archive_file)])
        os.remove(temp_archive_file)
        # Upload the archive file to Google Drive
        for root, dirs, files in os.walk(temp_dir_path):
            for file in files:
                media = MediaFileUpload(pathlib.Path(root) / file, resumable=True)
                file_metadata = {"name": file, "parents": [folder["id"]]}
                new_file = self._call_create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                self.logger.debug("Uploaded file", name=file, id=new_file["id"])

    def get_subfolder(self, folder_id, name):
        query_template = (
            "mimeType='application/vnd.google-apps.folder' "
            "and name='{name}' "
            "and '{parent}' in parents "
            "and trashed=false"
        )

        try:
            resp = (
                self.drive.files()
                .list(
                    q=query_template.format(name=name, parent=folder_id),
                    spaces="drive",
                    fields="files(id, name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageSize=1,
                )
                .execute()
            )
            items = resp.get("files", [])
            if items:
                return items[0]
            return None
        except HttpError as e:
            print("❌ API error:", e)
            raise e


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Upload a file to a Google Drive folder using a service account.")
    args, logger = init_application(
        "er-scarecrow-upload",
        "Upload files to Google Drive",
        get_parser,
    )
    service = DriveService(args.service_account_file, logger, args)
    root = service.verify_shared_drive(args.folder_id)
    if args.check:
        return
    if args.upload:
        if args.upload_archive:
            dest = service.get_or_create_subfolders(root, *pathlib.Path(args.upload_directory).parts)
            service.upload_archive(args.upload_archive, dest)
        elif args.upload_directory:
            dest = service.get_or_create_subfolders(root, *pathlib.Path(args.upload_directory).parts)
            service.upload_hierachy(args.upload_root, dest, args.upload_local_directory)
        else:
            parser.error("Either --upload-archive or --upload-directory must be specified.")


def get_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-s",
        "--service-account-file",
        type=str,
        help="Path to the service account JSON key file.(default:/etc/er-scarecrow-upload/google-service-key.json",
    ),
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
        help="path to a json file containing the root folder mappings, or a "
        "json object string (default:/etc/er-scarecrow-upload/root_mapping.json",
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
        "--upload-cleaunup",
        action="store_true",
        help="Cleanup the upload artifacts after successful upload.",
        default=False,
    )
    return parser


if __name__ == "__main__":
    main()
